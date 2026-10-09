// SPDX-License-Identifier: MIT
// pipeline — push inputs: feed a running pipeline from your own code.
//
//   auto [port, feeder] = rx.input<IqBlock>("in", 32);
//   ... build the rest of the chain on `port`, start it ...
//   feeder.tryPush(std::move(block));   // from your capture / channelizer thread
//   feeder.close();                     // end of stream: the chain drains and finishes
#pragma once

#include "block.hpp"
#include "channel.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

namespace pipeline {

namespace detail {

template <typename T>
struct FeedState {
  explicit FeedState(std::size_t capacity) : queue(capacity) {}
  Channel<T> queue;
  std::atomic<std::uint64_t> pushed{0};
  std::atomic<std::uint64_t> dropped{0};
};

}  // namespace detail

/// Handle used to push items into a pipeline created with Pipeline::input().
/// Cheap to copy; all copies feed the same input. Thread safe. Stays valid
/// (pushes simply fail) after the pipeline has stopped or been destroyed.
template <typename T>
class Feeder {
 public:
  Feeder() = default;
  explicit Feeder(std::shared_ptr<detail::FeedState<T>> s) : s_(std::move(s)) {}

  bool valid() const { return s_ != nullptr; }

  /// Blocks while the input queue is full (backpressure reaches the caller).
  /// Returns false once the input is closed or the pipeline stopped.
  bool push(T&& v) { return count(s_->queue.push(std::move(v))); }
  bool push(const T& v) { return push(T(v)); }

  /// Never blocks. Returns false if the queue is full (the item is counted as
  /// dropped and left untouched) or the input is closed. Use this from a
  /// real-time thread that must not be held up by one slow receiver.
  bool tryPush(T&& v) {
    if (s_->queue.tryPush(std::move(v))) return count(true);
    if (!s_->queue.closed()) s_->dropped.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  bool tryPush(const T& v) {
    T copy(v);
    return tryPush(std::move(copy));
  }

  /// Waits up to `timeout` for room; counts a drop on timeout.
  template <typename Rep, typename Period>
  bool pushFor(T&& v, std::chrono::duration<Rep, Period> timeout) {
    if (s_->queue.pushFor(std::move(v), timeout)) return count(true);
    if (!s_->queue.closed()) s_->dropped.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  /// End of stream. Items already queued are still processed; the pipeline
  /// then flushes and finishes on its own.
  void close() {
    if (s_) s_->queue.close();
  }
  /// True after close(), or once the pipeline stopped accepting input.
  bool closed() const { return !s_ || s_->queue.closed(); }

  std::size_t backlog() const { return s_ ? s_->queue.size() : 0; }
  std::size_t capacity() const { return s_ ? s_->queue.capacity() : 0; }
  std::uint64_t pushed() const { return s_ ? s_->pushed.load() : 0; }
  std::uint64_t dropped() const { return s_ ? s_->dropped.load() : 0; }

 private:
  bool count(bool ok) {
    if (ok) s_->pushed.fetch_add(1, std::memory_order_relaxed);
    return ok;
  }
  std::shared_ptr<detail::FeedState<T>> s_;
};

namespace detail {

/// The source behind Pipeline::input(): forwards what the Feeder pushes.
template <typename T>
class PushSource final : public Source<T> {
 public:
  explicit PushSource(std::shared_ptr<FeedState<T>> s) : s_(std::move(s)) {}
  // Also covers a pipeline destroyed without ever being started.
  ~PushSource() override { s_->queue.close(); }

  bool produce(Emitter<T>& out) override {
    if (this->stopRequested()) s_->queue.close();  // graceful stop: take no more, drain the rest
    std::optional<T> item;
    const PopStatus st = s_->queue.popFor(item, std::chrono::milliseconds(50));
    backlog_ = static_cast<std::int64_t>(s_->queue.size());
    dropped_ = static_cast<std::int64_t>(s_->dropped.load());
    if (st == PopStatus::Closed) return false;
    if (st == PopStatus::Item) out.emit(std::move(*item));
    return true;
  }
  bool drainOnStop() const override { return true; }
  // Whatever ends the source (close, stop, abort, error), later pushes must
  // fail instead of blocking forever.
  void onStop() override { s_->queue.close(); }

 private:
  std::shared_ptr<FeedState<T>> s_;
  Monitor<std::int64_t>& backlog_ = this->template monitor<std::int64_t>("backlog", "items waiting in the input", Aggregate::First);
  Monitor<std::int64_t>& dropped_ =
      this->template monitor<std::int64_t>("dropped", "items rejected because the input was full", Aggregate::First);
};

}  // namespace detail
}  // namespace pipeline
