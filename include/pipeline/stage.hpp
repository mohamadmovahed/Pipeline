// SPDX-License-Identifier: MIT
// pipeline — Stage: runs one block on one or more worker threads.
//
// A stage owns
//   * its input queue (bounded, see channel.hpp),
//   * one Block instance per worker ("replica"),
//   * a reorder buffer so that a parallel stage emits results in input order,
//   * statistics (rates, utilization, backpressure) and parameters.
#pragma once

#include "block.hpp"
#include "channel.hpp"
#include "parameters.hpp"
#include "snapshot.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace pipeline {

/// What a stage does when process() throws.
enum class ErrorPolicy {
  Skip,  ///< count the error, drop the item, keep running (default)
  Stop   ///< abort the whole pipeline
};

/// Per-stage configuration. Use the fluent setters:
///   opts().parallel(4)          4 workers, fixed
///   opts().autoscale(1, 8)      1..8 workers, chosen at run time
///   opts().queue(256)           input queue capacity (items)
///   opts().unordered()          parallel results may leave out of order
struct StageOptions {
  std::size_t workers = 1;          ///< initial (and minimum) worker count
  std::size_t maxWorkers = 1;       ///< > workers enables autoscaling
  std::size_t queueCapacity = 64;   ///< input queue capacity in items
  bool ordered = true;              ///< keep output order with parallel workers
  ErrorPolicy errorPolicy = ErrorPolicy::Skip;
  std::size_t window = 0;           ///< max items held for reordering, 0 = auto

  StageOptions& parallel(std::size_t n) {
    workers = maxWorkers = (std::max<std::size_t>)(1, n);
    return *this;
  }
  StageOptions& autoscale(std::size_t minWorkers, std::size_t maxW) {
    workers = (std::max<std::size_t>)(1, minWorkers);
    maxWorkers = (std::max)(workers, maxW);
    return *this;
  }
  StageOptions& queue(std::size_t capacity) {
    queueCapacity = (std::max<std::size_t>)(1, capacity);
    return *this;
  }
  StageOptions& unordered() {
    ordered = false;
    return *this;
  }
  StageOptions& onError(ErrorPolicy p) {
    errorPolicy = p;
    return *this;
  }
  StageOptions& reorderWindow(std::size_t items) {
    window = items;
    return *this;
  }
};

inline StageOptions opts() { return {}; }

class Pipeline;

class Stage {
 public:
  using Factory = std::function<std::unique_ptr<BlockBase>()>;

  Stage(std::string name, StageKind kind, StageOptions o, Factory factory)
      : name_(std::move(name)), kind_(kind), opts_(o), factory_(std::move(factory)) {
    if (kind_ == StageKind::Source) opts_.workers = opts_.maxWorkers = 1;  // sources are sequential
    opts_.workers = (std::max<std::size_t>)(1, opts_.workers);
    opts_.maxWorkers = (std::max)(opts_.workers, opts_.maxWorkers);
    if (opts_.window == 0) opts_.window = 4 * opts_.maxWorkers + 4;
    std::lock_guard<std::mutex> lk(m_);
    for (std::size_t i = 0; i < opts_.workers; ++i) createReplica();
  }
  Stage(const Stage&) = delete;
  Stage& operator=(const Stage&) = delete;
  virtual ~Stage() = default;

  const std::string& name() const { return name_; }
  StageKind kind() const { return kind_; }
  const StageOptions& options() const { return opts_; }

  /// Number of active workers.
  std::size_t workers() const { return active_.load(); }

  /// Manually sets the worker count, clamped to [1, maxWorkers].
  std::size_t setWorkers(std::size_t n) {
    n = std::clamp<std::size_t>(n, 1, opts_.maxWorkers);
    {
      std::lock_guard<std::mutex> lk(m_);
      if (!started_) {
        while (replicas_.size() < n) createReplica();
        while (replicas_.size() > n) replicas_.pop_back();
        active_ = replicas_.size();
        return n;
      }
    }
    while (workers() < n && addWorker()) {
    }
    while (workers() > n && removeWorker(1)) {
    }
    return workers();
  }

  /// Sets a control on every replica (and on replicas created later).
  void setControl(const std::string& control, const Value& v) {
    std::lock_guard<std::mutex> lk(m_);
    for (auto& r : replicas_) {
      auto* c = r->block->parameters().findControl(control);
      if (!c) throw std::invalid_argument("stage '" + name_ + "' has no control '" + control + "'");
      c->request(v);  // the first replica validates; a throw changes nothing
    }
    overrides_[control] = v;
  }

  /// Same, for a plain C++ value: setControl("iterations", 12).
  template <typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value>>>
  void setControl(const std::string& control, T&& v) {
    setControl(control, makeValue(std::forward<T>(v)));
  }

  /// Parses `text` according to the control's type, then sets it.
  void setControlFromString(const std::string& control, const std::string& text) {
    ValueType t;
    {
      std::lock_guard<std::mutex> lk(m_);
      auto* c = replicas_.front()->block->parameters().findControl(control);
      if (!c) throw std::invalid_argument("stage '" + name_ + "' has no control '" + control + "'");
      t = c->type();
    }
    setControl(control, parseValue(t, text));
  }

  Value getControl(const std::string& control) const {
    std::lock_guard<std::mutex> lk(m_);
    auto* c = replicas_.front()->block->parameters().findControl(control);
    if (!c) throw std::invalid_argument("stage '" + name_ + "' has no control '" + control + "'");
    return c->value();
  }

  /// Monitor value aggregated over the replicas (see Aggregate).
  Value getMonitor(const std::string& monitor) const {
    std::lock_guard<std::mutex> lk(m_);
    auto* m = replicas_.front()->block->parameters().findMonitor(monitor);
    if (!m) throw std::invalid_argument("stage '" + name_ + "' has no monitor '" + monitor + "'");
    return monitorValueLocked(monitor, m->aggregate());
  }

  bool finished() const { return finished_.load(); }
  double utilization() const {
    std::lock_guard<std::mutex> lk(statsM_);
    return util_;
  }
  double inputFill() const {
    std::size_t cap = queueCapacity();
    return cap ? static_cast<double>(queueSize()) / static_cast<double>(cap) : 0.0;
  }

  StageSnapshot snapshot() const {
    StageSnapshot s;
    s.name = name_;
    s.kind = kind_;
    s.workers = active_.load();
    s.minWorkers = opts_.workers;
    s.maxWorkers = opts_.maxWorkers;
    s.itemsIn = itemsIn_.load();
    s.itemsOut = itemsOut_.load();
    s.errors = errors_.load();
    {
      std::lock_guard<std::mutex> lk(statsM_);
      s.inRate = inRate_;
      s.outRate = outRate_;
      s.utilization = util_;
      s.backpressure = backpressure_;
      s.lastError = lastError_;
    }
    s.queueSize = queueSize();
    s.queueCapacity = queueCapacity();
    s.finished = finished_.load();

    std::lock_guard<std::mutex> lk(m_);
    const BlockBase* first = replicas_.front()->block.get();
    for (auto& r : replicas_)
      if (!r->retire) {
        first = r->block.get();
        break;
      }
    for (auto& c : first->parameters().controls()) {
      ParamSnapshot p;
      p.name = c->name();
      p.description = c->description();
      p.unit = c->unit();
      p.type = c->type();
      p.value = c->value();
      p.min = c->range().min;
      p.max = c->range().max;
      s.controls.push_back(std::move(p));
    }
    for (auto& m : first->parameters().monitors()) {
      ParamSnapshot p;
      p.name = m->name();
      p.description = m->description();
      p.unit = m->unit();
      p.type = m->type();
      p.aggregate = m->aggregate();
      p.value = monitorValueLocked(m->name(), m->aggregate());
      s.monitors.push_back(std::move(p));
    }
    return s;
  }

 protected:
  struct Replica {
    std::unique_ptr<BlockBase> block;
    std::thread thread;
    std::atomic<bool> retire{false};
    std::atomic<bool> exited{false};
  };

  /// The worker loop. Return when the input ended, the replica must retire
  /// (check r.retire between items) or the stage was aborted.
  virtual void run(Replica& r) = 0;
  /// Emits what the replica still buffers (end of stream or retirement).
  virtual void flushReplica(Replica& r) = 0;
  virtual void closeOutput() {}
  virtual void abortChannels() {}
  virtual std::size_t queueSize() const { return 0; }
  virtual std::size_t queueCapacity() const { return 0; }
  /// Throws if the stage is not fully connected.
  virtual void validate() const {}

  static std::uint64_t nowNs() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
  }

  /// Applies staged control changes; call before each item.
  static void beforeItem(Replica& r) { r.block->parameters().applyPending(); }
  void markInputDone() { inputDone_.store(true); }

  void handleError(const std::string& what) {
    errors_.fetch_add(1);
    {
      std::lock_guard<std::mutex> lk(statsM_);
      lastError_ = what;
    }
    if (opts_.errorPolicy == ErrorPolicy::Stop && onFatal_) onFatal_(name_ + ": " + what);
  }

  /// Runs `fn`, routing exceptions to handleError().
  template <typename F>
  void guarded(F&& fn) {
    try {
      fn();
    } catch (const std::exception& e) {
      handleError(e.what());
    } catch (...) {
      handleError("unknown exception");
    }
  }

  std::atomic<std::uint64_t> itemsIn_{0};
  std::atomic<std::uint64_t> itemsOut_{0};
  std::atomic<std::uint64_t> busyNs_{0};
  std::atomic<std::uint64_t> blockedNs_{0};
  std::atomic<std::uint64_t> errors_{0};
  std::atomic<bool> stop_{false};
  std::atomic<bool> aborted_{false};

 private:
  friend class Pipeline;

  // ---- lifecycle, driven by Pipeline -------------------------------------

  void start(std::function<void()> onFinished, std::function<void(const std::string&)> onFatal) {
    validate();
    onFinished_ = std::move(onFinished);
    onFatal_ = std::move(onFatal);
    std::lock_guard<std::mutex> lk(m_);
    started_ = true;
    active_ = alive_ = replicas_.size();
    for (auto& r : replicas_) launch(*r);
  }

  void requestStop() { stop_.store(true); }

  void abort() {
    aborted_.store(true);
    stop_.store(true);
    abortChannels();
  }

  void join() {
    std::vector<std::thread> threads;
    {
      std::lock_guard<std::mutex> lk(m_);
      for (auto& r : replicas_)
        if (r->thread.joinable()) threads.push_back(std::move(r->thread));
    }
    for (auto& t : threads) t.join();
  }

  /// Joins and destroys retired workers.
  void reap() {
    std::vector<std::unique_ptr<Replica>> dead;
    {
      std::lock_guard<std::mutex> lk(m_);
      if (finalizing_) return;
      for (auto it = replicas_.begin(); it != replicas_.end();) {
        if ((*it)->retire && (*it)->exited) {
          // Keep the contribution of retired workers to Sum monitors (counters).
          for (auto& m : (*it)->block->parameters().monitors())
            if (m->aggregate() == Aggregate::Sum) {
              auto rs = retiredSums_.find(m->name());
              retiredSums_[m->name()] =
                  rs == retiredSums_.end() ? m->value() : aggregate({rs->second, m->value()}, Aggregate::Sum);
            }
          dead.push_back(std::move(*it));
          it = replicas_.erase(it);
        } else {
          ++it;
        }
      }
    }
    for (auto& r : dead)
      if (r->thread.joinable()) r->thread.join();
  }

  bool addWorker() {
    std::lock_guard<std::mutex> lk(m_);
    if (!started_ || inputDone_ || finalizing_ || aborted_ || stop_ || active_ >= opts_.maxWorkers) return false;
    Replica& r = createReplica();
    ++active_;
    ++alive_;
    launch(r);
    return true;
  }

  bool removeWorker(std::size_t minimum) {
    std::lock_guard<std::mutex> lk(m_);
    if (!started_ || finalizing_ || active_ <= (std::max<std::size_t>)(1, minimum)) return false;
    for (auto it = replicas_.rbegin(); it != replicas_.rend(); ++it) {
      if (!(*it)->retire && !(*it)->exited) {
        (*it)->retire = true;
        --active_;
        return true;
      }
    }
    return false;
  }

  /// Recomputes rates; called periodically by the pipeline supervisor.
  void updateRates(double dt) {
    if (dt <= 0) return;
    const std::uint64_t in = itemsIn_.load(), out = itemsOut_.load(), busy = busyNs_.load(), blocked = blockedNs_.load();
    const double n = static_cast<double>((std::max<std::size_t>)(1, active_.load()));
    const double span = dt * 1e9 * n;
    std::lock_guard<std::mutex> lk(statsM_);
    const double a = haveRates_ ? 0.5 : 1.0;  // exponential smoothing
    auto ema = [a](double old, double now) { return old + a * (now - old); };
    inRate_ = ema(inRate_, static_cast<double>(in - lastIn_) / dt);
    outRate_ = ema(outRate_, static_cast<double>(out - lastOut_) / dt);
    util_ = ema(util_, (std::min)(1.0, static_cast<double>(busy - lastBusy_) / span));
    backpressure_ = ema(backpressure_, (std::min)(1.0, static_cast<double>(blocked - lastBlocked_) / span));
    lastIn_ = in;
    lastOut_ = out;
    lastBusy_ = busy;
    lastBlocked_ = blocked;
    haveRates_ = true;
  }

  // ---- internals -----------------------------------------------------------

  Replica& createReplica() {  // m_ held
    auto r = std::make_unique<Replica>();
    r->block = factory_();
    if (!r->block) throw std::logic_error("stage '" + name_ + "': factory returned null");
    r->block->replica_ = nextReplica_++;
    r->block->stop_ = &stop_;
    for (auto& [n, v] : overrides_)
      if (auto* c = r->block->parameters().findControl(n)) c->request(v);
    replicas_.push_back(std::move(r));
    return *replicas_.back();
  }

  void launch(Replica& r) {  // m_ held
    r.thread = std::thread([this, rp = &r] { workerMain(*rp); });
  }

  void workerMain(Replica& r) {
    guarded([&] {
      beforeItem(r);
      r.block->onStart();
    });
    run(r);
    if (r.retire && !aborted_) flushReplica(r);
    guarded([&] { r.block->onStop(); });

    bool last = false;
    {
      std::lock_guard<std::mutex> lk(m_);
      --alive_;
      if (alive_ == 0 && inputDone_ && !finalizing_) last = finalizing_ = true;
    }
    if (last) finalize();
    r.exited = true;
  }

  /// Runs once, on the last worker to leave: flush, close, notify.
  void finalize() {
    if (!aborted_) {
      // finalizing_ is set: no replica is added or reaped from now on.
      for (auto& r : replicas_)
        if (!r->retire) flushReplica(*r);
    }
    closeOutput();
    finished_.store(true);
    if (onFinished_) onFinished_();
  }

  /// Active replicas for Mean/Min/Max/First; every replica ever run for Sum.
  Value monitorValueLocked(const std::string& name, Aggregate agg) const {  // m_ held
    std::vector<Value> vals;
    for (auto& r : replicas_) {
      if (r->retire && agg != Aggregate::Sum) continue;
      if (auto* m = r->block->parameters().findMonitor(name)) vals.push_back(m->value());
    }
    if (agg == Aggregate::Sum) {
      auto it = retiredSums_.find(name);
      if (it != retiredSums_.end()) vals.push_back(it->second);
    }
    return aggregate(vals, agg);
  }

  static Value aggregate(const std::vector<Value>& vals, Aggregate agg) {
    if (vals.empty()) return Value(std::int64_t{0});
    const ValueType t = typeOf(vals.front());
    if (agg == Aggregate::First || t == ValueType::String || t == ValueType::Bool || vals.size() == 1)
      return vals.front();
    if (t == ValueType::Int && agg != Aggregate::Mean) {
      std::int64_t acc = std::get<std::int64_t>(vals.front());
      for (std::size_t i = 1; i < vals.size(); ++i) {
        std::int64_t v = std::get<std::int64_t>(vals[i]);
        acc = agg == Aggregate::Sum ? acc + v : agg == Aggregate::Min ? (std::min)(acc, v) : (std::max)(acc, v);
      }
      return Value(acc);
    }
    double acc = toDouble(vals.front());
    for (std::size_t i = 1; i < vals.size(); ++i) {
      double v = toDouble(vals[i]);
      acc = (agg == Aggregate::Sum || agg == Aggregate::Mean) ? acc + v
            : agg == Aggregate::Min                           ? (std::min)(acc, v)
                                                              : (std::max)(acc, v);
    }
    if (agg == Aggregate::Mean) acc /= static_cast<double>(vals.size());
    return Value(acc);
  }

  const std::string name_;
  const StageKind kind_;
  StageOptions opts_;
  Factory factory_;
  std::function<void()> onFinished_;
  std::function<void(const std::string&)> onFatal_;

  mutable std::mutex m_;  // guards replicas_, overrides_, counters below
  std::vector<std::unique_ptr<Replica>> replicas_;
  std::map<std::string, Value> overrides_;
  std::map<std::string, Value> retiredSums_;
  std::size_t nextReplica_ = 0;
  std::atomic<std::size_t> active_{0};  // non-retired workers
  std::size_t alive_ = 0;               // threads still inside run()
  bool started_ = false;
  bool finalizing_ = false;
  std::atomic<bool> inputDone_{false};
  std::atomic<bool> finished_{false};

  mutable std::mutex statsM_;
  bool haveRates_ = false;
  double inRate_ = 0, outRate_ = 0, util_ = 0, backpressure_ = 0;
  std::uint64_t lastIn_ = 0, lastOut_ = 0, lastBusy_ = 0, lastBlocked_ = 0;
  std::string lastError_;
};

namespace detail {

/// Output side of a stage: assigns dense sequence numbers and pushes.
template <typename T>
struct OutputPort {
  std::shared_ptr<Channel<Packet<T>>> channel;
  std::string owner;
  std::mutex m;
  std::uint64_t seq = 0;
  std::atomic<std::uint64_t>* counter = nullptr;

  /// Pushes every item (moving from them) and clears `items`, keeping its
  /// capacity. Returns false if the channel was closed or aborted.
  bool pushAll(std::vector<T>& items) {
    if (items.empty()) return true;
    bool ok = true;
    {
      std::lock_guard<std::mutex> lk(m);
      for (auto& x : items) {
        if (!channel->push(Packet<T>{seq++, std::move(x)})) {
          ok = false;
          break;
        }
        counter->fetch_add(1, std::memory_order_relaxed);
      }
    }
    items.clear();
    return ok;
  }
  bool pushAll(std::vector<T>&& items) { return pushAll(items); }
};

constexpr auto kPollInterval = std::chrono::milliseconds(50);

template <typename Out>
class SourceStage final : public Stage {
 public:
  SourceStage(std::string name, StageOptions o, Factory f)
      : Stage(std::move(name), StageKind::Source, o, std::move(f)) {
    port_.owner = this->name();
    port_.counter = &itemsOut_;
  }
  OutputPort<Out>& output() { return port_; }

 protected:
  void run(Replica& r) override {
    auto& src = static_cast<Source<Out>&>(*r.block);
    Emitter<Out> em;  // reused for every call: no allocation in steady state
    while (!aborted_) {
      if (stop_ && !src.drainOnStop()) break;
      beforeItem(r);
      bool more = true;
      const auto t0 = nowNs();
      guarded([&] { more = src.produce(em); });
      const auto t1 = nowNs();
      busyNs_ += t1 - t0;
      itemsIn_.fetch_add(em.size());
      const bool ok = port_.pushAll(em.items());
      blockedNs_ += nowNs() - t1;
      if (!ok || !more) break;
    }
    markInputDone();
  }
  void flushReplica(Replica&) override {}
  void closeOutput() override { port_.channel->close(); }
  void abortChannels() override {
    if (port_.channel) port_.channel->abort();
  }
  void validate() const override {
    if (!port_.channel) throw std::logic_error("output of stage '" + name() + "' is not connected");
  }

 private:
  OutputPort<Out> port_;
};

template <typename In, typename Out>
class BlockStage final : public Stage {
 public:
  BlockStage(std::string name, StageOptions o, Factory f)
      : Stage(std::move(name), StageKind::Block, o, std::move(f)),
        input_(std::make_shared<Channel<Packet<In>>>(options().queueCapacity)) {
    port_.owner = this->name();
    port_.counter = &itemsOut_;
  }
  std::shared_ptr<Channel<Packet<In>>> input() const { return input_; }
  OutputPort<Out>& output() { return port_; }

 protected:
  void run(Replica& r) override {
    auto& blk = static_cast<Block<In, Out>&>(*r.block);
    std::optional<Packet<In>> pkt;
    Emitter<Out> em;  // reused for every item: no allocation in steady state
    while (!r.retire) {
      const PopStatus st = input_->popFor(pkt, kPollInterval);
      if (st == PopStatus::Timeout) continue;
      if (st == PopStatus::Closed) {
        markInputDone();
        break;
      }
      itemsIn_.fetch_add(1);
      beforeItem(r);
      const auto t0 = nowNs();
      guarded([&] { blk.process(std::move(pkt->data), em); });
      const auto t1 = nowNs();
      busyNs_ += t1 - t0;
      const bool ok = deliver(pkt->seq, em);
      blockedNs_ += nowNs() - t1;
      if (!ok) {
        markInputDone();
        break;
      }
    }
  }

  void flushReplica(Replica& r) override {
    auto& blk = static_cast<Block<In, Out>&>(*r.block);
    Emitter<Out> em;
    guarded([&] { blk.flush(em); });
    std::lock_guard<std::mutex> lk(reorderM_);  // don't interleave with an ordered release
    port_.pushAll(em.items());
  }
  void closeOutput() override { port_.channel->close(); }
  void abortChannels() override {
    input_->abort();
    if (port_.channel) port_.channel->abort();
    std::lock_guard<std::mutex> lk(reorderM_);
    reorderCv_.notify_all();
  }
  std::size_t queueSize() const override { return input_->size(); }
  std::size_t queueCapacity() const override { return input_->capacity(); }
  void validate() const override {
    if (!port_.channel) throw std::logic_error("output of stage '" + name() + "' is not connected");
  }

 private:
  /// Forwards the outputs of input `seq`. In ordered mode results are held
  /// until all earlier inputs are done. A worker that is too far ahead of the
  /// oldest unfinished item waits, which bounds the reorder memory.
  ///
  /// A stage that can never have more than one worker is ordered by
  /// construction, so it skips the reorder buffer (no lock, no allocation).
  bool deliver(std::uint64_t seq, Emitter<Out>& em) {
    if (!options().ordered || options().maxWorkers == 1) return port_.pushAll(em.items());
    std::unique_lock<std::mutex> lk(reorderM_);
    reorderCv_.wait(lk, [&] { return seq < next_ + options().window || aborted_; });
    if (aborted_) {
      em.clear();
      return false;
    }
    pending_.emplace(seq, em.take());
    bool ok = true;
    for (auto it = pending_.find(next_); it != pending_.end(); it = pending_.find(next_)) {
      std::vector<Out> v = std::move(it->second);
      pending_.erase(it);
      ++next_;
      if (!port_.pushAll(std::move(v))) {
        ok = false;
        break;
      }
    }
    lk.unlock();
    reorderCv_.notify_all();
    return ok;
  }

  std::shared_ptr<Channel<Packet<In>>> input_;
  OutputPort<Out> port_;
  std::mutex reorderM_;
  std::condition_variable reorderCv_;
  std::map<std::uint64_t, std::vector<Out>> pending_;
  std::uint64_t next_ = 0;
};

template <typename In>
class SinkStage final : public Stage {
 public:
  SinkStage(std::string name, StageOptions o, Factory f)
      : Stage(std::move(name), StageKind::Sink, o, std::move(f)),
        input_(std::make_shared<Channel<Packet<In>>>(options().queueCapacity)) {}
  std::shared_ptr<Channel<Packet<In>>> input() const { return input_; }

 protected:
  void run(Replica& r) override {
    auto& snk = static_cast<Sink<In>&>(*r.block);
    std::optional<Packet<In>> pkt;
    while (!r.retire) {
      const PopStatus st = input_->popFor(pkt, kPollInterval);
      if (st == PopStatus::Timeout) continue;
      if (st == PopStatus::Closed) {
        markInputDone();
        break;
      }
      itemsIn_.fetch_add(1);
      beforeItem(r);
      const auto t0 = nowNs();
      guarded([&] { snk.consume(std::move(pkt->data)); });
      busyNs_ += nowNs() - t0;
    }
  }
  void flushReplica(Replica& r) override {
    auto& snk = static_cast<Sink<In>&>(*r.block);
    guarded([&] { snk.flush(); });
  }
  void abortChannels() override { input_->abort(); }
  std::size_t queueSize() const override { return input_->size(); }
  std::size_t queueCapacity() const override { return input_->capacity(); }

 private:
  std::shared_ptr<Channel<Packet<In>>> input_;
};

}  // namespace detail
}  // namespace pipeline
