// SPDX-License-Identifier: MIT
// pipeline — PipelinePool: many pipelines that come and go at run time, e.g.
// one receiver chain per active channel of a wideband front end.
//
//   PipelinePool pool("receivers");
//   pool.start("ch7", buildReceiver(...));   // builds threads, returns immediately
//   pool.stop("ch7");                        // drains in the background, returns immediately
//   pool.snapshotJson();                     // every running receiver, for the UI
//
// Stopping never blocks the caller: a background thread joins finished
// pipelines and reports them through onFinished(). The id can be reused at
// once, even while the old pipeline with that id is still draining.
#pragma once

#include "graph.hpp"
#include "json.hpp"
#include "snapshot.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace pipeline {

struct PoolSnapshot {
  std::string name;
  std::uint64_t started = 0;   ///< pipelines started so far
  std::uint64_t finished = 0;  ///< pipelines finished and joined so far
  std::size_t stopping = 0;    ///< pipelines currently draining after stop()
  struct Entry {
    std::string id;
    PipelineSnapshot pipeline;
  };
  std::vector<Entry> active;  ///< running pipelines, sorted by id

  const PipelineSnapshot* find(const std::string& id) const {
    for (auto& e : active)
      if (e.id == id) return &e.pipeline;
    return nullptr;
  }
};

inline std::string toJson(const PoolSnapshot& p) {
  std::string o = "{\"name\":" + jsonEscape(p.name) + ",\"started\":" + std::to_string(p.started) +
                  ",\"finished\":" + std::to_string(p.finished) + ",\"stopping\":" + std::to_string(p.stopping) +
                  ",\"active\":[";
  for (std::size_t i = 0; i < p.active.size(); ++i)
    o += (i ? "," : "") + std::string("{\"id\":") + jsonEscape(p.active[i].id) +
         ",\"pipeline\":" + toJson(p.active[i].pipeline) + "}";
  return o + "]}";
}

class PipelinePool {
 public:
  /// Called on the pool's thread after a pipeline finished and was joined,
  /// whatever the reason (stop(), abort(), its input closed, a fatal error).
  using FinishedFn = std::function<void(const std::string& id, const PipelineSnapshot& last)>;

  explicit PipelinePool(std::string name = "pool",
                        std::chrono::milliseconds reapPeriod = std::chrono::milliseconds(100))
      : name_(std::move(name)), period_(reapPeriod) {
    reaper_ = std::thread([this] { reap(); });
  }
  PipelinePool(const PipelinePool&) = delete;
  PipelinePool& operator=(const PipelinePool&) = delete;

  /// Aborts whatever is still running and joins everything.
  ~PipelinePool() {
    abortAll();
    {
      std::lock_guard<std::mutex> lk(m_);
      quit_ = true;
    }
    cv_.notify_all();
    reaper_.join();
  }

  void onFinished(FinishedFn fn) {
    std::lock_guard<std::mutex> lk(m_);
    onFinished_ = std::move(fn);
  }

  /// Starts a fully built (not yet started) pipeline under `id`.
  /// Throws std::logic_error if `id` is already running.
  std::shared_ptr<Pipeline> start(const std::string& id, std::unique_ptr<Pipeline> p) {
    if (!p) throw std::invalid_argument("PipelinePool::start: null pipeline");
    std::lock_guard<std::mutex> lk(m_);
    if (active_.count(id)) throw std::logic_error("pool '" + name_ + "': '" + id + "' is already running");
    std::shared_ptr<Pipeline> sp(std::move(p));
    sp->start();
    active_.emplace(id, sp);
    ++started_;
    return sp;
  }

  /// Graceful, non-blocking stop: inputs are drained and flushed in the
  /// background. Returns false if `id` is not running.
  bool stop(const std::string& id) { return retire(id, false); }
  /// Immediate, non-blocking stop: queued data is dropped.
  bool abort(const std::string& id) { return retire(id, true); }

  void stopAll() {
    for (auto& id : ids()) stop(id);
  }
  void abortAll() {
    for (auto& id : ids()) abort(id);
  }

  /// Blocks until no pipeline is running or draining. Pipelines that never
  /// end by themselves must be stopped first (stopAll()).
  void waitAll() {
    std::unique_lock<std::mutex> lk(m_);
    idleCv_.wait(lk, [&] { return active_.empty() && retiring_.empty() && reaping_ == 0; });
  }

  /// The running pipeline with this id, or nullptr.
  std::shared_ptr<Pipeline> get(const std::string& id) const {
    std::lock_guard<std::mutex> lk(m_);
    auto it = active_.find(id);
    return it == active_.end() ? nullptr : it->second;
  }
  bool contains(const std::string& id) const { return get(id) != nullptr; }

  std::vector<std::string> ids() const {
    std::lock_guard<std::mutex> lk(m_);
    std::vector<std::string> v;
    for (auto& kv : active_) v.push_back(kv.first);
    return v;
  }
  std::size_t size() const {
    std::lock_guard<std::mutex> lk(m_);
    return active_.size();
  }

  /// setControl("ch7", "decoder.iterations", 12). Returns false if `id` is not running.
  bool setControl(const std::string& id, const std::string& path, const Value& v) {
    auto p = get(id);
    if (!p) return false;
    p->setControl(path, v);
    return true;
  }
  template <typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Value>>>
  bool setControl(const std::string& id, const std::string& path, T&& v) {
    return setControl(id, path, makeValue(std::forward<T>(v)));
  }

  PoolSnapshot snapshot() const {
    PoolSnapshot s;
    s.name = name_;
    std::vector<std::pair<std::string, std::shared_ptr<Pipeline>>> list;
    {
      std::lock_guard<std::mutex> lk(m_);
      s.started = started_;
      s.finished = finished_;
      s.stopping = retiring_.size() + reaping_;
      list.assign(active_.begin(), active_.end());
    }
    for (auto& [id, p] : list) s.active.push_back({id, p->snapshot()});
    return s;
  }
  std::string snapshotJson() const { return toJson(snapshot()); }

  const std::string& name() const { return name_; }

 private:
  using Entry = std::pair<std::string, std::shared_ptr<Pipeline>>;

  bool retire(const std::string& id, bool hard) {
    std::lock_guard<std::mutex> lk(m_);
    auto it = active_.find(id);
    if (it == active_.end()) return false;
    hard ? it->second->requestAbort() : it->second->requestStop();
    retiring_.push_back(*it);
    active_.erase(it);
    return true;
  }

  void reap() {
    for (;;) {
      std::vector<Entry> done;
      {
        std::unique_lock<std::mutex> lk(m_);
        cv_.wait_for(lk, period_);
        // pipelines that ended by themselves (input closed, source ended, fatal error)
        for (auto it = active_.begin(); it != active_.end();) {
          if (it->second->done()) {
            done.push_back(*it);
            it = active_.erase(it);
          } else {
            ++it;
          }
        }
        for (auto it = retiring_.begin(); it != retiring_.end();) {
          if (it->second->done()) {
            done.push_back(*it);
            it = retiring_.erase(it);
          } else {
            ++it;
          }
        }
        reaping_ += done.size();
        if (quit_ && done.empty() && active_.empty() && retiring_.empty()) return;
      }
      for (auto& [id, p] : done) {
        p->wait();  // joins threads; quick, every stage has finished
        const PipelineSnapshot last = p->snapshot();
        FinishedFn fn;
        {
          std::lock_guard<std::mutex> lk(m_);
          fn = onFinished_;
        }
        if (fn) {
          try {
            fn(id, last);
          } catch (...) {
          }
        }
        p.reset();
        std::lock_guard<std::mutex> lk(m_);
        --reaping_;
        ++finished_;
      }
      idleCv_.notify_all();
    }
  }

  std::string name_;
  std::chrono::milliseconds period_;
  mutable std::mutex m_;
  std::condition_variable cv_;
  std::condition_variable idleCv_;
  std::map<std::string, std::shared_ptr<Pipeline>> active_;
  std::vector<Entry> retiring_;
  std::size_t reaping_ = 0;
  std::uint64_t started_ = 0;
  std::uint64_t finished_ = 0;
  bool quit_ = false;
  FinishedFn onFinished_;
  std::thread reaper_;
};

}  // namespace pipeline
