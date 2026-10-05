// Receiver chain example.
//
//   tx_sim ──> demod ──> decoder (1..N workers, autoscaled) ──> datalink ──> app
//
// * tx_sim   : simulates the RF front end: random frames + CRC, rate-1/3
//              repetition code, BPSK, AWGN at a configurable SNR.
// * demod    : BPSK soft demodulator with SNR/EVM estimation (cheap).
// * decoder  : soft repetition decoder with an "iterations" knob that makes
//              it expensive (stands in for an LDPC/turbo decoder). It is the
//              bottleneck, so the autoscaler gives it more workers.
// * datalink : CRC check, drops bad frames, packs N frames into one packet
//              (rate change N:1), checks that frames arrive in order.
// * app      : counts delivered bytes.
//
// A console "UI" prints a dashboard every second and changes controls while
// the chain is running.
//
// usage: receiver_chain [--seconds N] [--max-workers N] [--json]
#include <pipeline/pipeline.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <complex>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace pipeline;
using namespace std::chrono_literals;

namespace rx {

// ---- data types flowing between the blocks -------------------------------

struct IqBlock {
  std::uint64_t frame = 0;
  std::vector<std::complex<float>> samples;
};

struct SoftFrame {
  std::uint64_t frame = 0;
  std::vector<float> llr;  // > 0 means bit 0
};

struct DecodedFrame {
  std::uint64_t frame = 0;
  std::vector<std::uint8_t> bytes;  // payload + 2 CRC bytes
};

struct NetPacket {
  std::uint64_t firstFrame = 0;
  int frames = 0;
  std::vector<std::uint8_t> payload;
};

constexpr int kPayloadBytes = 64;
constexpr int kRepetition = 3;

inline std::uint16_t crc16(const std::uint8_t* p, std::size_t n) {
  std::uint16_t crc = 0xFFFF;
  for (std::size_t i = 0; i < n; ++i) {
    crc ^= static_cast<std::uint16_t>(p[i] << 8);
    for (int b = 0; b < 8; ++b) crc = (crc & 0x8000) ? static_cast<std::uint16_t>((crc << 1) ^ 0x1021) : crc << 1;
  }
  return crc;
}

// ---- blocks -----------------------------------------------------------------

class TxSimulator : public Source<IqBlock> {
 public:
  bool produce(Emitter<IqBlock>& out) override {
    if (rate_.get() > 0) {  // optional real-time pacing
      next_ += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          std::chrono::duration<double>(1.0 / rate_.get()));
      std::this_thread::sleep_until(next_);
    } else {
      next_ = std::chrono::steady_clock::now();
    }

    std::vector<std::uint8_t> bytes(kPayloadBytes + 2);
    for (int i = 0; i < kPayloadBytes; ++i) bytes[i] = static_cast<std::uint8_t>(rng_());
    const std::uint16_t crc = crc16(bytes.data(), kPayloadBytes);
    bytes[kPayloadBytes] = static_cast<std::uint8_t>(crc >> 8);
    bytes[kPayloadBytes + 1] = static_cast<std::uint8_t>(crc & 0xFF);

    const double snr = std::pow(10.0, snrDb_.get() / 10.0);  // Es/N0 per coded bit
    std::normal_distribution<float> noise(0.0f, static_cast<float>(std::sqrt(1.0 / (2.0 * snr))));

    IqBlock blk;
    blk.frame = frame_++;
    blk.samples.reserve(bytes.size() * 8 * kRepetition);
    for (std::uint8_t byte : bytes)
      for (int b = 7; b >= 0; --b) {
        const float s = ((byte >> b) & 1) ? -1.0f : 1.0f;
        for (int r = 0; r < kRepetition; ++r) blk.samples.emplace_back(s + noise(rng_), noise(rng_));
      }
    out.emit(std::move(blk));
    ++sent_;
    return true;  // endless; the application calls stop()
  }

 private:
  Control<double>& snrDb_ = control<double>("snr_db", 6.0, "channel Es/N0 per coded bit", {-10.0, 30.0}, "dB");
  Control<double>& rate_ = control<double>("frame_rate", 4000.0, "frames per second, 0 = as fast as possible",
                                           {0.0, 1e6}, "frames/s");
  Monitor<std::int64_t>& sent_ = monitor<std::int64_t>("frames_sent", "frames generated", Aggregate::Sum);
  std::mt19937 rng_{12345};
  std::uint64_t frame_ = 0;
  std::chrono::steady_clock::time_point next_ = std::chrono::steady_clock::now();
};

class Demodulator : public Block<IqBlock, SoftFrame> {
 public:
  void process(IqBlock&& in, Emitter<SoftFrame>& out) override {
    // Amplitude and noise estimation from the in-phase component.
    double a = 0;
    for (auto& s : in.samples) a += std::abs(s.real());
    a /= static_cast<double>(in.samples.size());
    double var = 0, evm = 0;
    for (auto& s : in.samples) {
      const double e = std::abs(s.real()) - a;
      var += e * e + static_cast<double>(s.imag()) * s.imag();
      evm += std::norm(s - std::complex<float>(s.real() > 0 ? static_cast<float>(a) : -static_cast<float>(a), 0));
    }
    var /= 2.0 * static_cast<double>(in.samples.size());  // per real dimension
    snrEst_ = 10.0 * std::log10(a * a / (2.0 * var));
    evm_ = 100.0 * std::sqrt(evm / static_cast<double>(in.samples.size())) / a;

    SoftFrame f;
    f.frame = in.frame;
    f.llr.resize(in.samples.size());
    const float scale = static_cast<float>(2.0 * a / var * gain_.get());
    for (std::size_t i = 0; i < in.samples.size(); ++i) f.llr[i] = scale * in.samples[i].real();
    out.emit(std::move(f));
  }

 private:
  Control<double>& gain_ = control<double>("llr_gain", 1.0, "extra LLR scaling", {0.01, 100.0});
  Monitor<double>& snrEst_ = monitor<double>("snr_est", "estimated Es/N0", Aggregate::Mean, "dB");
  Monitor<double>& evm_ = monitor<double>("evm", "error vector magnitude", Aggregate::Mean, "%");
};

class Decoder : public Block<SoftFrame, DecodedFrame> {
 public:
  void process(SoftFrame&& in, Emitter<DecodedFrame>& out) override {
    const std::size_t nbits = in.llr.size() / kRepetition;
    DecodedFrame d;
    d.frame = in.frame;
    d.bytes.assign(nbits / 8, 0);
    std::int64_t corrected = 0;
    // "iterations" re-runs the soft combining to emulate an expensive
    // iterative decoder. Only the last pass is used.
    for (int it = 0; it < iterations_.get(); ++it) {
      corrected = 0;
      for (std::size_t b = 0; b < nbits; ++b) {
        float sum = 0;
        int hardOnes = 0;
        for (int r = 0; r < kRepetition; ++r) {
          const float l = in.llr[b * kRepetition + r] * (1.0f + 1e-7f * static_cast<float>(it));
          sum += l;
          hardOnes += l < 0;
        }
        const bool one = sum < 0;
        if (hardOnes != 0 && hardOnes != kRepetition) ++corrected;
        if (it + 1 == iterations_.get() && one) d.bytes[b / 8] |= static_cast<std::uint8_t>(0x80 >> (b % 8));
      }
    }
    corrected_ += corrected;
    ++frames_;
    out.emit(std::move(d));
  }

 private:
  Control<int>& iterations_ = control<int>("iterations", 300, "decoder passes (CPU cost per frame)", {1, 1000});
  Monitor<std::int64_t>& corrected_ =
      monitor<std::int64_t>("corrected_bits", "bits fixed by the code", Aggregate::Sum);
  Monitor<std::int64_t>& frames_ = monitor<std::int64_t>("frames", "frames decoded", Aggregate::Sum);
};

class DataLink : public Block<DecodedFrame, NetPacket> {
 public:
  void process(DecodedFrame&& in, Emitter<NetPacket>& out) override {
    if (expected_ != in.frame) ++outOfOrder_;  // would fire if ordering were broken
    expected_ = in.frame + 1;

    const std::uint16_t crc = crc16(in.bytes.data(), kPayloadBytes);
    const bool ok = in.bytes[kPayloadBytes] == (crc >> 8) && in.bytes[kPayloadBytes + 1] == (crc & 0xFF);
    ok ? ++crcOk_ : ++crcFail_;
    const double total = static_cast<double>(crcOk_.get() + crcFail_.get());
    fer_ = total > 0 ? static_cast<double>(crcFail_.get()) / total : 0.0;
    if (!ok) return;  // drop: zero outputs for this input

    if (pending_.frames == 0) pending_.firstFrame = in.frame;
    pending_.payload.insert(pending_.payload.end(), in.bytes.begin(), in.bytes.begin() + kPayloadBytes);
    if (++pending_.frames >= framesPerPacket_.get()) flush(out);
  }

  void flush(Emitter<NetPacket>& out) override {
    if (pending_.frames == 0) return;
    out.emit(std::move(pending_));
    pending_ = NetPacket{};
  }

 private:
  Control<int>& framesPerPacket_ = control<int>("frames_per_packet", 4, "good frames per network packet", {1, 64});
  Monitor<std::int64_t>& crcOk_ = monitor<std::int64_t>("crc_ok", "frames with valid CRC", Aggregate::Sum);
  Monitor<std::int64_t>& crcFail_ = monitor<std::int64_t>("crc_fail", "frames with bad CRC", Aggregate::Sum);
  Monitor<double>& fer_ = monitor<double>("fer", "frame error rate", Aggregate::Mean);
  Monitor<std::int64_t>& outOfOrder_ = monitor<std::int64_t>("out_of_order", "frames out of order", Aggregate::Sum);
  NetPacket pending_;
  std::uint64_t expected_ = 0;
};

class AppSink : public Sink<NetPacket> {
 public:
  void consume(NetPacket&& p) override {
    ++packets_;
    bytes_ += static_cast<std::int64_t>(p.payload.size());
  }

 private:
  Monitor<std::int64_t>& packets_ = monitor<std::int64_t>("packets", "packets delivered", Aggregate::Sum);
  Monitor<std::int64_t>& bytes_ = monitor<std::int64_t>("bytes", "payload bytes delivered", Aggregate::Sum, "B");
};

}  // namespace rx

// ---- a console "UI" -----------------------------------------------------------

static std::string shortValue(const Value& v) {
  if (typeOf(v) != ValueType::Double) return toString(v);
  char buf[32];
  std::snprintf(buf, sizeof buf, "%.3g", std::get<double>(v));
  return buf;
}

static void printDashboard(const PipelineSnapshot& s) {
  std::printf("\n== %s  [%s]  t=%.1fs  bottleneck: %s\n", s.name.c_str(), toString(s.state), s.uptime,
              s.bottleneck.c_str());
  std::printf("%-9s %7s %10s %10s %6s %6s %9s  %s\n", "stage", "workers", "in/s", "out/s", "util", "wait",
              "queue", "monitors");
  for (auto& st : s.stages) {
    std::string mons;
    for (auto& m : st.monitors) mons += m.name + "=" + shortValue(m.value) + m.unit + " ";
    char q[32];
    std::snprintf(q, sizeof q, "%zu/%zu", st.queueSize, st.queueCapacity);
    char w[16];
    std::snprintf(w, sizeof w, "%zu/%zu", st.workers, st.maxWorkers);
    std::printf("%-9s %7s %10.1f %10.1f %5.0f%% %5.0f%% %9s  %s\n", st.name.c_str(), w, st.inRate, st.outRate,
                100 * st.utilization, 100 * st.backpressure, st.kind == StageKind::Source ? "-" : q, mons.c_str());
  }
  std::fflush(stdout);
}

int main(int argc, char** argv) {
  double seconds = 9;
  bool json = false;
  std::size_t maxWorkers = std::max(2u, std::thread::hardware_concurrency());
  for (int i = 1; i < argc; ++i) {
    if (!std::strcmp(argv[i], "--seconds") && i + 1 < argc) seconds = std::atof(argv[++i]);
    else if (!std::strcmp(argv[i], "--max-workers") && i + 1 < argc) maxWorkers = std::strtoul(argv[++i], nullptr, 10);
    else if (!std::strcmp(argv[i], "--json")) json = true;
    else {
      std::fprintf(stderr, "usage: %s [--seconds N] [--max-workers N] [--json]\n", argv[0]);
      return 2;
    }
  }

  PipelineOptions po;
  po.monitorPeriod = 250ms;
  po.scaleCooldown = 500ms;
  Pipeline rxChain("receiver", po);

  auto iq = rxChain.source<rx::TxSimulator>("tx_sim");
  auto soft = rxChain.add<rx::Demodulator>("demod", iq, opts().queue(16));
  auto bits = rxChain.add<rx::Decoder>("decoder", soft, opts().autoscale(1, maxWorkers).queue(64));
  auto pkts = rxChain.add<rx::DataLink>("datalink", bits, opts().queue(64));
  rxChain.sink<rx::AppSink>("app", pkts);

  int tick = 0;
  rxChain.onUpdate([&](const PipelineSnapshot& s) {
    if (++tick % 4 == 0) printDashboard(s);  // every second
  });

  rxChain.start();
  const auto t0 = std::chrono::steady_clock::now();
  auto at = [&](double sec) { std::this_thread::sleep_until(t0 + std::chrono::duration<double>(sec)); };

  at(seconds / 3);
  std::printf("\n>>> UI: tx_sim.snr_db = 2.5 dB (worse channel)\n");
  rxChain.setControl("tx_sim.snr_db", 2.5);

  at(2 * seconds / 3);
  std::printf("\n>>> UI: decoder.iterations = 60 (cheaper decoder), datalink.frames_per_packet = 8\n");
  rxChain.setControl("decoder.iterations", 60);
  rxChain.setControlFromString("datalink.frames_per_packet", "8");

  at(seconds);
  rxChain.stop();  // graceful: drain and flush everything in flight
  printDashboard(rxChain.snapshot());
  if (json) std::cout << "\n" << rxChain.snapshotJson() << "\n";

  const auto ooo = std::get<std::int64_t>(rxChain.getMonitor("datalink.out_of_order"));
  std::printf("\nframes out of order at the data link: %lld (expected 0)\n", static_cast<long long>(ooo));
  return ooo == 0 ? 0 : 1;
}
