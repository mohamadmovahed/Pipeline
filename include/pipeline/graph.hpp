// SPDX-License-Identifier: MIT
// pipeline — Pipeline: builds a chain of stages, runs it, scales it and
// exposes it to a user interface.
#pragma once

#include "block.hpp"
#include "json.hpp"
#include "snapshot.hpp"
#include "stage.hpp"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace pipeline {

struct PipelineOptions {
  /// How often rates are computed, the autoscaler runs and onUpdate fires.
  std::chrono::milliseconds monitorPeriod{250};
  /// Enable automatic worker scaling for stages with maxWorkers > workers.
  bool autoscale = true;
  /// Add a worker when utilization and input-queue fill are both above these.
  double scaleUpUtilization = 0.80;
  double scaleUpQueueFill = 0.50;
  /// Remove a worker when utilization and input-queue fill are both below these.
  double scaleDownUtilization = 0.30;
  double scaleDownQueueFill = 0.05;
  /// Minimum time between two scaling decisions of the same stage.
  std::chrono::milliseconds scaleCooldown{1000};
};

/// Typed handle to the output of a stage; pass it to the next stage.
template <typename T>
class Port {
 public:
  Port() = default;
  bool valid() const { return port_ != nullptr; }
  Stage& stage() const { return *stage_; }

 private:
  friend class Pipeline;
  Port(detail::OutputPort<T>* p, Stage* s) : port_(p), stage_(s) {}
  detail::OutputPort<T>* port_ = nullptr;
  Stage* stage_ = nullptr;
};

namespace detail {

template <typename Out, typename F>
class FnSource final : public Source<Out> {
 public:
  explicit FnSource(F f) : f_(std::move(f)) {}
  bool produce(Emitter<Out>& out) override { return f_(out); }

 private:
  F f_;
};

template <typename In, typename Out, typename F>
class FnBlock final : public Block<In, Out> {
 public:
  explicit FnBlock(F f) : f_(std::move(f)) {}
  void process(In&& in, Emitter<Out>& out) override { f_(std::move(in), out); }

 private:
  F f_;
};

template <typename In, typename Out, typename F>
class MapBlock final : public Block<In, Out> {
 public:
  explicit MapBlock(F f) : f_(std::move(f)) {}
  void process(In&& in, Emitter<Out>& out) override { out.emit(f_(std::move(in))); }

 private:
  F f_;
};

template <typename In, typename F>
class FnSink final : public Sink<In> {
 public:
  explicit FnSink(F f) : f_(std::move(f)) {}
  void consume(In&& in) override { f_(std::move(in)); }

 private:
  F f_;
};

}  // namespace detail

class Pipeline {
 public:
  using UpdateFn = std::function<void(const PipelineSnapshot&)>;

  explicit Pipeline(std::string name = "pipeline", PipelineOptions o = {}) : name_(std::move(name)), opts_(o) {}
  Pipeline(const Pipeline&) = delete;
  Pipeline& operator=(const Pipeline&) = delete;
  ~Pipeline() {
    if (state_ != PipelineState::Idle) abort();
  }

  // ---- building ------------------------------------------------------------
  // Class-based stages: the block type B is constructed with `args` (copied
  // for every replica of a parallel stage).

  template <typename B, typename... Args>
  Port<typename B::output_type> source(const std::string& name, Args&&... args) {
    using Out = typename B::output_type;
    static_assert(std::is_base_of_v<Source<Out>, B>, "source<B>: B must derive from pipeline::Source<Out>");
    auto& st = addStage(
        std::make_unique<detail::SourceStage<Out>>(name, StageOptions{}, factory<B>(std::forward<Args>(args)...)));
    return Port<Out>(&st.output(), &st);
  }

  template <typename B>
  Port<typename B::output_type> add(const std::string& name, const Port<typename B::input_type>& from) {
    return add<B>(name, from, StageOptions{});
  }

  template <typename B, typename... Args>
  Port<typename B::output_type> add(const std::string& name, const Port<typename B::input_type>& from,
                                    const StageOptions& o, Args&&... args) {
    using In = typename B::input_type;
    using Out = typename B::output_type;
    static_assert(std::is_base_of_v<Block<In, Out>, B>, "add<B>: B must derive from pipeline::Block<In, Out>");
    auto st = std::make_unique<detail::BlockStage<In, Out>>(name, o, factory<B>(std::forward<Args>(args)...));
    auto& ref = connectAndAdd(from, std::move(st));
    return Port<Out>(&ref.output(), &ref);
  }

  template <typename B>
  Stage& sink(const std::string& name, const Port<typename B::input_type>& from) {
    return sink<B>(name, from, StageOptions{});
  }

  template <typename B, typename... Args>
  Stage& sink(const std::string& name, const Port<typename B::input_type>& from, const StageOptions& o,
              Args&&... args) {
    using In = typename B::input_type;
    static_assert(std::is_base_of_v<Sink<In>, B>, "sink<B>: B must derive from pipeline::Sink<In>");
    return connectAndAdd(from,
                         std::make_unique<detail::SinkStage<In>>(name, o, factory<B>(std::forward<Args>(args)...)));
  }

  // Function-based stages: wrap existing processing-library calls without
  // writing a class. The callable is copied for every replica.

  /// f(Emitter<Out>&) -> bool (false = end of stream)
  template <typename Out, typename F>
  Port<Out> sourceFn(const std::string& name, F f) {
    return source<detail::FnSource<Out, F>>(name, std::move(f));
  }

  /// f(In&&, Emitter<Out>&) — any number of outputs per input.
  template <typename Out, typename In, typename F>
  Port<Out> blockFn(const std::string& name, const Port<In>& from, F f, const StageOptions& o = {}) {
    return add<detail::FnBlock<In, Out, F>>(name, from, o, std::move(f));
  }

  /// f(In&&) -> Out — exactly one output per input.
  template <typename In, typename F>
  auto map(const std::string& name, const Port<In>& from, F f, const StageOptions& o = {}) {
    using Out = std::decay_t<std::invoke_result_t<F&, In&&>>;
    return add<detail::MapBlock<In, Out, F>>(name, from, o, std::move(f));
  }

  /// f(In&&)
  template <typename In, typename F>
  Stage& sinkFn(const std::string& name, const Port<In>& from, F f, const StageOptions& o = {}) {
    return sink<detail::FnSink<In, F>>(name, from, o, std::move(f));
  }

  // ---- running -------------------------------------------------------------

  /// Starts all worker threads. A pipeline runs once; build a new one to rerun.
  void start() {
    std::lock_guard<std::mutex> lk(m_);
    if (state_ != PipelineState::Idle) throw std::logic_error("pipeline '" + name_ + "' was already started");
    if (stages_.empty()) throw std::logic_error("pipeline '" + name_ + "' has no stages");
    for (auto& s : stages_) s->validate();
    startTime_ = std::chrono::steady_clock::now();
    lastScale_.assign(stages_.size(), startTime_);
    state_ = PipelineState::Running;
    for (auto& s : stages_) {
      s->start([this] { stageFinished(); }, [this](const std::string& e) { fatal(e); });
    }
    supervisor_ = std::thread([this] { supervise(); });
  }

  /// Graceful stop: sources stop producing, everything in flight is processed
  /// and flushed, then threads are joined.
  void stop() {
    if (state_ == PipelineState::Idle) return;
    if (state_ == PipelineState::Running) state_ = PipelineState::Stopping;
    for (auto& s : stages_)
      if (s->kind() == StageKind::Source) s->requestStop();
    wait();
  }

  /// Immediate stop: queued data is dropped, then threads are joined.
  void abort() {
    if (state_ == PipelineState::Idle) return;
    aborted_ = true;
    for (auto& s : stages_) s->abort();
    wait();
  }

  /// Blocks until every stage finished (end of stream, stop() or abort()).
  void wait() {
    std::lock_guard<std::mutex> jg(joinM_);
    if (state_ == PipelineState::Idle || joined_) return;
    {
      std::unique_lock<std::mutex> lk(m_);
      doneCv_.wait(lk, [&] { return finished_ == stages_.size(); });
      supStop_ = true;
    }
    supCv_.notify_all();
    if (supervisor_.joinable()) supervisor_.join();
    for (auto& s : stages_) s->join();
    endTime_ = std::chrono::steady_clock::now();
    state_ = aborted_ ? PipelineState::Aborted : PipelineState::Finished;
    joined_ = true;
  }

  /// Waits up to `timeout`; returns true (after joining) if the pipeline finished.
  template <typename Rep, typename Period>
  bool waitFor(std::chrono::duration<Rep, Period> timeout) {
    {
      std::unique_lock<std::mutex> lk(m_);
      if (state_ == PipelineState::Idle) return true;
      if (!doneCv_.wait_for(lk, timeout, [&] { return finished_ == stages_.size(); })) return false;
    }
    wait();
    return true;
  }

  PipelineState state() const { return state_; }
  bool running() const { return state_ == PipelineState::Running || state_ == PipelineState::Stopping; }

  // ---- user interface ------------------------------------------------------

  /// Called on the supervisor thread every monitorPeriod while running.
  /// Set it before start(). Keep it short (copy the data, post to the UI thread).
  void onUpdate(UpdateFn fn) {
    if (state_ != PipelineState::Idle) throw std::logic_error("onUpdate() must be called before start()");
    onUpdate_ = std::move(fn);
  }

  PipelineSnapshot snapshot() const {
    PipelineSnapshot p;
    p.name = name_;
    p.state = state_;
    if (state_ != PipelineState::Idle) {
      auto end = joined_ ? endTime_ : std::chrono::steady_clock::now();
      p.uptime = std::chrono::duration<double>(end - startTime_).count();
    }
    {
      std::lock_guard<std::mutex> lk(errM_);
      p.error = error_;
    }
    double best = -1;
    for (auto& s : stages_) {
      p.stages.push_back(s->snapshot());
      const auto& ss = p.stages.back();
      if (ss.kind != StageKind::Source && ss.utilization > best) {
        best = ss.utilization;
        p.bottleneck = ss.name;
      }
    }
    return p;
  }

  std::string snapshotJson() const { return toJson(snapshot()); }

  /// Name of the busiest non-source stage (the one limiting throughput).
  std::string bottleneck() const { return snapshot().bottleneck; }

  /// Sets a control: setControl("decoder.iterations", 12). Thread safe.
  void setControl(const std::string& path, const Value& v) {
    auto [st, ctl] = split(path);
    stage(st).setControl(ctl, v);
  }
  /// Same, parsing text according to the control type (handy for UIs / CLIs).
  void setControlFromString(const std::string& path, const std::string& text) {
    auto [st, ctl] = split(path);
    stage(st).setControlFromString(ctl, text);
  }
  Value getControl(const std::string& path) const {
    auto [st, ctl] = split(path);
    return stage(st).getControl(ctl);
  }
  Value getMonitor(const std::string& path) const {
    auto [st, mon] = split(path);
    return stage(st).getMonitor(mon);
  }

  Stage& stage(const std::string& name) const {
    for (auto& s : stages_)
      if (s->name() == name) return *s;
    throw std::invalid_argument("pipeline '" + name_ + "' has no stage '" + name + "'");
  }
  std::vector<Stage*> stages() const {
    std::vector<Stage*> v;
    for (auto& s : stages_) v.push_back(s.get());
    return v;
  }

  const std::string& name() const { return name_; }
  const PipelineOptions& options() const { return opts_; }

 private:
  template <typename B, typename... Args>
  static Stage::Factory factory(Args&&... args) {
    return [tup = std::make_tuple(std::forward<Args>(args)...)]() -> std::unique_ptr<BlockBase> {
      return std::apply([](const auto&... a) -> std::unique_ptr<BlockBase> { return std::make_unique<B>(a...); },
                        tup);
    };
  }

  template <typename S>
  S& addStage(std::unique_ptr<S> s) {
    if (state_ != PipelineState::Idle) throw std::logic_error("cannot add stages to a started pipeline");
    const std::string& n = s->name();
    if (n.empty() || n.find('.') != std::string::npos)
      throw std::invalid_argument("stage name '" + n + "' must be non-empty and contain no '.'");
    for (auto& e : stages_)
      if (e->name() == n) throw std::invalid_argument("duplicate stage name '" + n + "'");
    S& ref = *s;
    stages_.push_back(std::move(s));
    return ref;
  }

  template <typename T, typename S>
  S& connectAndAdd(const Port<T>& from, std::unique_ptr<S> s) {
    if (!from.valid()) throw std::invalid_argument("stage '" + s->name() + "': input port is not valid");
    bool mine = false;
    for (auto& e : stages_) mine = mine || e.get() == from.stage_;
    if (!mine) throw std::invalid_argument("stage '" + s->name() + "': input port belongs to another pipeline");
    if (from.port_->channel)
      throw std::logic_error("output of stage '" + from.port_->owner + "' is already connected; use one consumer per output");
    auto ch = s->input();
    S& ref = addStage(std::move(s));
    from.port_->channel = ch;
    return ref;
  }

  static std::pair<std::string, std::string> split(const std::string& path) {
    auto dot = path.find('.');
    if (dot == std::string::npos || dot == 0 || dot + 1 == path.size())
      throw std::invalid_argument("parameter path '" + path + "' must look like 'stage.parameter'");
    return {path.substr(0, dot), path.substr(dot + 1)};
  }

  void stageFinished() {
    {
      std::lock_guard<std::mutex> lk(m_);
      ++finished_;
    }
    doneCv_.notify_all();
  }

  void fatal(const std::string& e) {
    {
      std::lock_guard<std::mutex> lk(errM_);
      if (!error_.empty()) return;
      error_ = e;
    }
    aborted_ = true;
    for (auto& s : stages_) s->abort();  // non-blocking; wait() joins
  }

  void supervise() {
    lastUpdate_ = std::chrono::steady_clock::now();
    for (;;) {
      {
        std::unique_lock<std::mutex> lk(m_);
        supCv_.wait_for(lk, opts_.monitorPeriod, [&] { return supStop_; });
        if (supStop_) return;
      }
      const auto now = std::chrono::steady_clock::now();
      const double dt = std::chrono::duration<double>(now - lastUpdate_).count();
      lastUpdate_ = now;
      for (auto& s : stages_) s->updateRates(dt);
      if (opts_.autoscale && !aborted_) autoscale(now);
      for (auto& s : stages_) s->reap();
      if (onUpdate_) {
        try {
          onUpdate_(snapshot());
        } catch (...) {
        }
      }
    }
  }

  void autoscale(std::chrono::steady_clock::time_point now) {
    for (std::size_t i = 0; i < stages_.size(); ++i) {
      Stage& s = *stages_[i];
      const auto& o = s.options();
      if (s.kind() == StageKind::Source || o.maxWorkers <= o.workers || s.finished()) continue;
      if (now - lastScale_[i] < opts_.scaleCooldown) continue;
      const double u = s.utilization(), fill = s.inputFill();
      const std::size_t n = s.workers();
      bool changed = false;
      if (u >= opts_.scaleUpUtilization && fill >= opts_.scaleUpQueueFill && n < o.maxWorkers)
        changed = s.addWorker();
      else if (u <= opts_.scaleDownUtilization && fill <= opts_.scaleDownQueueFill && n > o.workers)
        changed = s.removeWorker(o.workers);
      if (changed) lastScale_[i] = now;
    }
  }

  std::string name_;
  PipelineOptions opts_;
  std::vector<std::unique_ptr<Stage>> stages_;
  std::atomic<PipelineState> state_{PipelineState::Idle};
  std::atomic<bool> aborted_{false};
  std::atomic<bool> joined_{false};

  mutable std::mutex m_;
  std::condition_variable doneCv_;
  std::condition_variable supCv_;
  std::size_t finished_ = 0;
  bool supStop_ = false;
  std::thread supervisor_;
  std::mutex joinM_;
  UpdateFn onUpdate_;

  mutable std::mutex errM_;
  std::string error_;

  std::chrono::steady_clock::time_point startTime_, endTime_, lastUpdate_;
  std::vector<std::chrono::steady_clock::time_point> lastScale_;
};

}  // namespace pipeline
