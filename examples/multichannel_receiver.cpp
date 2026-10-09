// Multichannel receiver template: receiver chains started on demand.
//
//   front end (one pipeline, always running)
//     sdr ──▶ channelizer ──▶ dispatcher
//                                 │  detects which channels are active
//                                 │  starts / stops one receiver per active channel
//                                 ▼
//   receivers (one pipeline per active channel, in a PipelinePool)
//     in ──▶ demod ──▶ decoder (1..4 workers) ──▶ datalink ──▶ output ──▶ application
//
// The DSP is stubbed out: every place where your algorithm goes is marked
// "TODO(dsp)". The structure (detection with hysteresis, receivers started on
// demand, feeding, graceful shutdown, controls and monitoring) is the part to
// keep.
//
// usage: multichannel_receiver [--seconds N] [--channels N] [--json]
#include <pipeline/pipeline.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace pipeline;
using namespace std::chrono_literals;
using cf32 = std::complex<float>;

// ============================================================================
// Data types
// ============================================================================

/// Raw wideband capture: `channels` channels interleaved sample by sample.
struct WidebandBlock {
  std::uint64_t seq = 0;
  int channels = 0;
  std::vector<cf32> samples;
};

/// Output of the channelizer: one narrowband stream per channel plus its power.
struct ChannelizedBlock {
  std::uint64_t seq = 0;
  std::vector<std::vector<cf32>> channel;  // [channel][sample]
  std::vector<double> powerDb;             // [channel]
};

/// What a per-channel receiver gets.
struct ChannelBlock {
  int channel = 0;
  std::uint64_t seq = 0;  // wideband block number, handy for timestamps
  std::vector<cf32> samples;
};

struct SoftBits {
  int channel = 0;
  std::vector<float> llr;
};

struct Bits {
  int channel = 0;
  std::vector<std::uint8_t> bits;
};

struct NetPacket {
  int channel = 0;
  std::vector<std::uint8_t> payload;
};

// ============================================================================
// Front end
// ============================================================================

/// Simulated SDR. Each channel randomly switches between idle (noise) and
/// active (a signal) so that the dispatcher has something to detect.
/// TODO(dsp): replace with your hardware / file reader.
class SimulatedSdr : public Source<WidebandBlock> {
 public:
  explicit SimulatedSdr(int channels) : channels_(channels), active_(channels, false) {}

  bool produce(Emitter<WidebandBlock>& out) override {
    // real-time pacing
    next_ += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double>(1.0 / blockRate_.get()));
    std::this_thread::sleep_until(next_);

    // channel activity: a random on/off process per channel
    std::uniform_real_distribution<double> u(0, 1);
    int nActive = 0;
    for (int c = 0; c < channels_; ++c) {
      const double p = active_[c] ? 1.0 / (blockRate_.get() * meanOnSeconds_.get())
                                  : 1.0 / (blockRate_.get() * meanOffSeconds_.get());
      if (u(rng_) < p) active_[c] = !active_[c];
      nActive += active_[c];
    }
    activeTruth_ = nActive;

    WidebandBlock b;
    b.seq = seq_++;
    b.channels = channels_;
    b.samples.resize(static_cast<std::size_t>(samplesPerChannel_.get()) * channels_);
    std::normal_distribution<float> noise(0.0f, 0.05f);
    std::bernoulli_distribution bit(0.5);
    for (std::size_t i = 0; i < b.samples.size(); ++i) {
      const int c = static_cast<int>(i % channels_);
      cf32 s(noise(rng_), noise(rng_));
      if (active_[c]) s += cf32(bit(rng_) ? 0.7f : -0.7f, bit(rng_) ? 0.7f : -0.7f);  // QPSK-ish
      b.samples[i] = s;
    }
    out.emit(std::move(b));
    return true;
  }

 private:
  int channels_;
  std::vector<bool> active_;
  std::mt19937 rng_{7};
  std::uint64_t seq_ = 0;
  std::chrono::steady_clock::time_point next_ = std::chrono::steady_clock::now();
  Control<double>& blockRate_ = control<double>("block_rate", 200.0, "wideband blocks per second", {1.0, 1e5}, "1/s");
  Control<int>& samplesPerChannel_ = control<int>("samples_per_channel", 1024, "samples per channel per block", {16, 1 << 20});
  Control<double>& meanOnSeconds_ = control<double>("mean_on", 3.0, "simulated mean burst length", {0.1, 1e4}, "s");
  Control<double>& meanOffSeconds_ = control<double>("mean_off", 4.0, "simulated mean idle time", {0.1, 1e4}, "s");
  Monitor<std::int64_t>& activeTruth_ = monitor<std::int64_t>("active_truth", "channels actually active", Aggregate::First);
};

/// Splits the wideband stream into channels and measures the power of each.
/// TODO(dsp): replace the de-interleaving with your channelizer (polyphase
/// filter bank, FFT, DDCs...). It is the most expensive front-end step, so it
/// is stateless here and can run on several workers (see main()).
class Channelizer : public Block<WidebandBlock, ChannelizedBlock> {
 public:
  void process(WidebandBlock&& in, Emitter<ChannelizedBlock>& out) override {
    const int n = in.channels;
    ChannelizedBlock c;
    c.seq = in.seq;
    c.channel.assign(n, {});
    c.powerDb.assign(n, 0.0);
    for (auto& ch : c.channel) ch.reserve(in.samples.size() / n);
    for (std::size_t i = 0; i < in.samples.size(); ++i) c.channel[i % n].push_back(in.samples[i]);
    for (int k = 0; k < n; ++k) {
      double p = 0;
      for (auto& s : c.channel[k]) p += std::norm(s);
      c.powerDb[k] = 10.0 * std::log10(p / std::max<std::size_t>(1, c.channel[k].size()) + 1e-12);
    }
    out.emit(std::move(c));
  }
};

// ============================================================================
// Per-channel receiver (stubs)
// ============================================================================

/// TODO(dsp): synchronisation, equalisation, soft demapping.
class Demodulator : public Block<ChannelBlock, SoftBits> {
 public:
  void process(ChannelBlock&& in, Emitter<SoftBits>& out) override {
    SoftBits s;
    s.channel = in.channel;
    s.llr.reserve(2 * in.samples.size());
    double p = 0;
    for (auto& x : in.samples) {
      s.llr.push_back(static_cast<float>(gain_.get()) * x.real());
      s.llr.push_back(static_cast<float>(gain_.get()) * x.imag());
      p += std::norm(x);
    }
    levelDb_ = 10.0 * std::log10(p / std::max<std::size_t>(1, in.samples.size()) + 1e-12);
    out.emit(std::move(s));
  }

 private:
  Control<double>& gain_ = control<double>("llr_gain", 1.0, "LLR scaling", {0.0, 100.0});
  Monitor<double>& levelDb_ = monitor<double>("level", "signal level", Aggregate::Mean, "dB");
};

/// TODO(dsp): your FEC decoder. `work_us` burns CPU to stand in for its cost,
/// so you can watch the autoscaler react.
class Decoder : public Block<SoftBits, Bits> {
 public:
  void process(SoftBits&& in, Emitter<Bits>& out) override {
    const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(workUs_.get());
    while (std::chrono::steady_clock::now() < until) {
    }
    Bits b;
    b.channel = in.channel;
    b.bits.reserve(in.llr.size());
    for (float l : in.llr) b.bits.push_back(l < 0 ? 1 : 0);
    ++frames_;
    out.emit(std::move(b));
  }

 private:
  Control<int>& workUs_ = control<int>("work_us", 6000, "simulated decoding cost per block", {0, 1000000}, "us");
  Monitor<std::int64_t>& frames_ = monitor<std::int64_t>("frames", "blocks decoded", Aggregate::Sum);
};

/// TODO(dsp): framing, CRC, reassembly. Packs bits into fixed-size packets
/// (an N:1 rate change) and emits the partial packet at end of burst.
class DataLink : public Block<Bits, NetPacket> {
 public:
  void process(Bits&& in, Emitter<NetPacket>& out) override {
    channel_ = in.channel;
    for (std::uint8_t bit : in.bits) {
      byte_ = static_cast<std::uint8_t>((byte_ << 1) | bit);
      if (++nbits_ == 8) {
        pending_.push_back(byte_);
        nbits_ = 0;
        if (static_cast<int>(pending_.size()) == packetBytes_.get()) emitPending(out);
      }
    }
  }
  void flush(Emitter<NetPacket>& out) override {  // end of burst
    if (!pending_.empty()) emitPending(out);
  }

 private:
  void emitPending(Emitter<NetPacket>& out) {
    out.emit(NetPacket{channel_, std::move(pending_)});
    pending_.clear();
    ++packets_;
  }
  Control<int>& packetBytes_ = control<int>("packet_bytes", 1500, "bytes per packet", {1, 65536}, "B");
  Monitor<std::int64_t>& packets_ = monitor<std::int64_t>("packets", "packets assembled", Aggregate::Sum);
  int channel_ = 0;
  std::vector<std::uint8_t> pending_;
  std::uint8_t byte_ = 0;
  int nbits_ = 0;
};

/// Hands packets to the application.
using PacketHandler = std::function<void(NetPacket&&)>;

class Output : public Sink<NetPacket> {
 public:
  explicit Output(PacketHandler handler) : handler_(std::move(handler)) {}
  void consume(NetPacket&& p) override {
    bytes_ += static_cast<std::int64_t>(p.payload.size());
    if (handler_) handler_(std::move(p));
  }

 private:
  PacketHandler handler_;
  Monitor<std::int64_t>& bytes_ = monitor<std::int64_t>("bytes", "bytes delivered", Aggregate::Sum, "B");
};

/// Settings that every new receiver starts with. The UI can change them; they
/// are applied to the receivers already running and to the ones started later.
class ReceiverDefaults {
 public:
  template <typename T>
  void set(const std::string& path, T&& v) {
    std::lock_guard<std::mutex> lk(m_);
    values_[path] = makeValue(std::forward<T>(v));
  }
  void applyTo(Pipeline& p) const {
    std::lock_guard<std::mutex> lk(m_);
    for (auto& [path, v] : values_) p.setControl(path, v);
  }

 private:
  mutable std::mutex m_;
  std::map<std::string, Value> values_;
};

/// Builds (does not start) the receiver chain of one channel.
std::unique_ptr<Pipeline> makeReceiver(int channel, Feeder<ChannelBlock>& feeder, const ReceiverDefaults& defaults,
                                       const PacketHandler& handler) {
  PipelineOptions po;
  po.monitorPeriod = 250ms;
  po.scaleCooldown = 500ms;
  auto rx = std::make_unique<Pipeline>("ch" + std::to_string(channel), po);

  auto in = rx->input<ChannelBlock>("in", 32);  // the dispatcher pushes here
  auto soft = rx->add<Demodulator>("demod", in.port);
  auto bits = rx->add<Decoder>("decoder", soft, opts().autoscale(1, 4).queue(16));
  auto pkts = rx->add<DataLink>("datalink", bits);
  rx->sink<Output>("output", pkts, opts(), handler);

  defaults.applyTo(*rx);
  feeder = in.feeder;
  return rx;
}

// ============================================================================
// Dispatcher: channel activity detection + receiver management
// ============================================================================

class Dispatcher : public Sink<ChannelizedBlock> {
 public:
  Dispatcher(PipelinePool* pool, const ReceiverDefaults* defaults, PacketHandler handler)
      : pool_(pool), defaults_(defaults), handler_(std::move(handler)) {}

  void consume(ChannelizedBlock&& in) override {
    const int n = static_cast<int>(in.channel.size());
    if (static_cast<int>(state_.size()) != n) state_.resize(n);

    for (int c = 0; c < n; ++c) {
      ChannelState& st = state_[c];

      // TODO(dsp): replace with your detector (energy, cyclostationary,
      // preamble correlation...). Hysteresis: `attack` blocks above the
      // on-threshold to start, `hang` blocks below the off-threshold to stop.
      const double p = in.powerDb[c];
      st.above = p > onDb_.get() ? st.above + 1 : 0;
      st.below = p < offDb_.get() ? st.below + 1 : 0;

      if (!st.running && st.above >= attack_.get()) startReceiver(c, st);
      if (st.running && st.below >= hang_.get()) stopReceiver(c, st);
      if (!st.running) continue;

      ChannelBlock blk{c, in.seq, std::move(in.channel[c])};
      const bool ok = dropWhenFull_.get() ? st.feeder.tryPush(std::move(blk)) : st.feeder.push(std::move(blk));
      if (!ok) {
        if (st.feeder.closed()) {  // the receiver ended by itself (e.g. fatal error): restart on next detection
          st.running = false;
          active_.add(-1);
        } else {
          ++dropped_;  // receiver overloaded: block dropped instead of stalling the other channels
        }
      }
    }
  }

  void flush() override {  // front end stopping: end every burst gracefully
    for (int c = 0; c < static_cast<int>(state_.size()); ++c)
      if (state_[c].running) stopReceiver(c, state_[c]);
  }

 private:
  struct ChannelState {
    bool running = false;
    int above = 0;
    int below = 0;
    Feeder<ChannelBlock> feeder;
  };

  static std::string id(int c) { return "ch" + std::to_string(c); }

  void startReceiver(int c, ChannelState& st) {
    try {
      pool_->start(id(c), makeReceiver(c, st.feeder, *defaults_, handler_));
      st.running = true;
      st.below = 0;
      ++active_;
      ++started_;
    } catch (const std::exception& e) {
      // e.g. a bad default control value; try again on the next detection
      std::fprintf(stderr, "cannot start receiver %s: %s\n", id(c).c_str(), e.what());
    }
  }

  void stopReceiver(int c, ChannelState& st) {
    pool_->stop(id(c));  // non-blocking: the receiver drains and flushes in the background
    st.feeder = {};
    st.running = false;
    st.above = 0;
    active_.add(-1);
    ++stopped_;
  }

  PipelinePool* pool_;
  const ReceiverDefaults* defaults_;
  PacketHandler handler_;
  std::vector<ChannelState> state_;

  Control<double>& onDb_ = control<double>("on_threshold", -10.0, "start a receiver above this power", {-200.0, 100.0}, "dB");
  Control<double>& offDb_ = control<double>("off_threshold", -15.0, "stop a receiver below this power", {-200.0, 100.0}, "dB");
  Control<int>& attack_ = control<int>("attack_blocks", 2, "blocks above on_threshold before starting", {1, 10000});
  Control<int>& hang_ = control<int>("hang_blocks", 40, "blocks below off_threshold before stopping", {1, 100000});
  Control<bool>& dropWhenFull_ =
      control<bool>("drop_when_full", true, "drop blocks for an overloaded receiver instead of stalling the front end");
  Monitor<std::int64_t>& active_ = monitor<std::int64_t>("active", "receivers running", Aggregate::First);
  Monitor<std::int64_t>& started_ = monitor<std::int64_t>("started", "receivers started", Aggregate::First);
  Monitor<std::int64_t>& stopped_ = monitor<std::int64_t>("stopped", "receivers stopped", Aggregate::First);
  Monitor<std::int64_t>& dropped_ = monitor<std::int64_t>("dropped", "blocks dropped (receiver full)", Aggregate::First);
};

// ============================================================================
// Application
// ============================================================================

namespace {

std::string monitorText(const PipelineSnapshot& p, const std::string& stage, const std::string& name) {
  if (auto* s = p.stage(stage))
    for (auto& m : s->monitors)
      if (m.name == name) return toString(m.value);
  return "-";
}

void printDashboard(const Pipeline& frontend, const PipelinePool& pool) {
  const PipelineSnapshot fe = frontend.snapshot();
  const PoolSnapshot rx = pool.snapshot();
  std::printf("\n== t=%.1fs  front end: %.0f blocks/s, bottleneck %s | active %s (truth %s) started %s stopped %s "
              "dropped %s | pool: %zu running, %zu draining, %llu finished\n",
              fe.uptime, fe.stages.front().outRate, fe.bottleneck.c_str(),
              monitorText(fe, "dispatcher", "active").c_str(), monitorText(fe, "sdr", "active_truth").c_str(),
              monitorText(fe, "dispatcher", "started").c_str(), monitorText(fe, "dispatcher", "stopped").c_str(),
              monitorText(fe, "dispatcher", "dropped").c_str(), rx.active.size(), rx.stopping,
              static_cast<unsigned long long>(rx.finished));
  std::printf("%-6s %8s %9s %8s %9s %8s %10s %11s\n", "rx", "uptime", "blocks/s", "backlog", "dec wrk", "dec util",
              "packets", "bytes");
  for (auto& e : rx.active) {
    const auto& p = e.pipeline;
    const auto* in = p.stage("in");
    const auto* dec = p.stage("decoder");
    std::printf("%-6s %7.1fs %9.1f %8s %5zu/%-3zu %7.0f%% %10s %11s\n", e.id.c_str(), p.uptime, in ? in->outRate : 0.0,
                monitorText(p, "in", "backlog").c_str(), dec ? dec->workers : 0, dec ? dec->maxWorkers : 0,
                dec ? 100 * dec->utilization : 0.0, monitorText(p, "datalink", "packets").c_str(),
                monitorText(p, "output", "bytes").c_str());
  }
  std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
  double seconds = 15;
  int channels = 8;
  bool json = false;
  for (int i = 1; i < argc; ++i) {
    if (!std::strcmp(argv[i], "--seconds") && i + 1 < argc) seconds = std::atof(argv[++i]);
    else if (!std::strcmp(argv[i], "--channels") && i + 1 < argc) channels = std::atoi(argv[++i]);
    else if (!std::strcmp(argv[i], "--json")) json = true;
    else {
      std::fprintf(stderr, "usage: %s [--seconds N] [--channels N] [--json]\n", argv[0]);
      return 2;
    }
  }

  // --- application side: where decoded packets end up ----------------------
  std::atomic<std::int64_t> appBytes{0};
  PacketHandler toApplication = [&appBytes](NetPacket&& p) {
    appBytes += static_cast<std::int64_t>(p.payload.size());  // TODO: hand to your application / network
  };

  // --- receivers: created on demand by the dispatcher ------------------------
  ReceiverDefaults defaults;
  defaults.set("decoder.work_us", 6000);
  PipelinePool pool("receivers");
  pool.onFinished([](const std::string& id, const PipelineSnapshot& last) {
    std::printf("   [%s finished after %.1fs: %s, %s packets]\n", id.c_str(), last.uptime, toString(last.state),
                monitorText(last, "datalink", "packets").c_str());
  });

  // --- front end: always running ------------------------------------------------
  PipelineOptions po;
  po.monitorPeriod = 250ms;
  Pipeline frontend("frontend", po);
  auto wb = frontend.source<SimulatedSdr>("sdr", channels);
  auto chans = frontend.add<Channelizer>("channelizer", wb, opts().autoscale(1, 2).queue(8));
  // The dispatcher keeps per-channel state: it must stay at one worker.
  frontend.sink<Dispatcher>("dispatcher", chans, opts().queue(8), &pool, &defaults, toApplication);

  frontend.start();
  const auto t0 = std::chrono::steady_clock::now();
  bool changed = false;
  while (std::chrono::steady_clock::now() - t0 < std::chrono::duration<double>(seconds)) {
    std::this_thread::sleep_for(1s);
    printDashboard(frontend, pool);

    if (!changed && std::chrono::steady_clock::now() - t0 > std::chrono::duration<double>(seconds / 2)) {
      changed = true;
      // A UI change that applies to every receiver, running or future:
      std::printf("\n>>> UI: decoder.work_us = 1000 on all receivers\n");
      defaults.set("decoder.work_us", 1000);
      for (auto& id : pool.ids()) pool.setControl(id, "decoder.work_us", 1000);
      // ... and one that changes the detector:
      frontend.setControl("dispatcher.hang_blocks", 20);
    }
  }

  // --- shutdown: stop the front end first; its dispatcher then stops every
  //     receiver gracefully (bursts are flushed), and we wait for them ------------
  frontend.stop();
  pool.stopAll();
  pool.waitAll();
  printDashboard(frontend, pool);
  if (json) std::cout << "\n" << pool.snapshotJson() << "\n";
  std::printf("\napplication received %lld bytes\n", static_cast<long long>(appBytes.load()));
  return 0;
}
