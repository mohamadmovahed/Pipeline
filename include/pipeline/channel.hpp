// SPDX-License-Identifier: MIT
// pipeline — bounded blocking queue connecting two stages.
//
// The bound is what makes rate matching automatic: a fast producer blocks when
// the queue in front of a slower consumer is full (backpressure), so the whole
// chain settles at the throughput of its slowest stage without dropping data
// and without unbounded memory growth.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>

namespace pipeline {

/// An item travelling between stages with its sequence number. Sequence
/// numbers are dense (0, 1, 2, ...) per connection; they let a parallel stage
/// put its results back in input order.
template <typename T>
struct Packet {
  std::uint64_t seq = 0;
  T data;
};

enum class PopStatus { Item, Timeout, Closed };

template <typename T>
class Channel {
 public:
  explicit Channel(std::size_t capacity) : capacity_(capacity ? capacity : 1) {}
  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  /// Blocks while the queue is full. Returns false if the channel was closed
  /// or aborted (the item is dropped).
  bool push(T&& item) {
    std::unique_lock<std::mutex> lk(m_);
    notFull_.wait(lk, [&] { return q_.size() < capacity_ || closed_ || aborted_; });
    if (closed_ || aborted_) return false;
    q_.push_back(std::move(item));
    if (q_.size() > highWater_) highWater_ = q_.size();
    lk.unlock();
    notEmpty_.notify_one();
    return true;
  }

  /// Non-blocking push. Returns false (and leaves `item` untouched) when the
  /// queue is full, closed or aborted.
  bool tryPush(T&& item) {
    std::unique_lock<std::mutex> lk(m_);
    if (closed_ || aborted_ || q_.size() >= capacity_) return false;
    q_.push_back(std::move(item));
    if (q_.size() > highWater_) highWater_ = q_.size();
    lk.unlock();
    notEmpty_.notify_one();
    return true;
  }

  /// Like push() but gives up after `timeout` (item untouched on failure).
  template <typename Rep, typename Period>
  bool pushFor(T&& item, std::chrono::duration<Rep, Period> timeout) {
    std::unique_lock<std::mutex> lk(m_);
    if (!notFull_.wait_for(lk, timeout, [&] { return q_.size() < capacity_ || closed_ || aborted_; })) return false;
    if (closed_ || aborted_) return false;
    q_.push_back(std::move(item));
    if (q_.size() > highWater_) highWater_ = q_.size();
    lk.unlock();
    notEmpty_.notify_one();
    return true;
  }

  bool closed() const {
    std::lock_guard<std::mutex> lk(m_);
    return closed_ || aborted_;
  }

  /// Blocks until an item is available. Returns nullopt once the channel is
  /// closed and drained, or aborted.
  std::optional<T> pop() {
    std::unique_lock<std::mutex> lk(m_);
    notEmpty_.wait(lk, [&] { return !q_.empty() || closed_ || aborted_; });
    return takeLocked(lk);
  }

  /// Like pop() but gives up after `timeout`.
  template <typename Rep, typename Period>
  PopStatus popFor(std::optional<T>& out, std::chrono::duration<Rep, Period> timeout) {
    std::unique_lock<std::mutex> lk(m_);
    if (!notEmpty_.wait_for(lk, timeout, [&] { return !q_.empty() || closed_ || aborted_; }))
      return PopStatus::Timeout;
    out = takeLocked(lk);
    return out ? PopStatus::Item : PopStatus::Closed;
  }

  /// End of stream: no more pushes, consumers drain what is queued.
  void close() {
    {
      std::lock_guard<std::mutex> lk(m_);
      closed_ = true;
    }
    notEmpty_.notify_all();
    notFull_.notify_all();
  }

  /// Emergency stop: wakes everybody and drops queued items.
  void abort() {
    std::deque<T> drop;
    {
      std::lock_guard<std::mutex> lk(m_);
      aborted_ = true;
      drop.swap(q_);
    }
    notEmpty_.notify_all();
    notFull_.notify_all();
  }

  std::size_t size() const {
    std::lock_guard<std::mutex> lk(m_);
    return q_.size();
  }
  std::size_t capacity() const { return capacity_; }
  std::size_t highWater() const {
    std::lock_guard<std::mutex> lk(m_);
    return highWater_;
  }

 private:
  std::optional<T> takeLocked(std::unique_lock<std::mutex>& lk) {
    if (aborted_ || q_.empty()) return std::nullopt;
    std::optional<T> v(std::move(q_.front()));
    q_.pop_front();
    lk.unlock();
    notFull_.notify_one();
    return v;
  }

  const std::size_t capacity_;
  mutable std::mutex m_;
  std::condition_variable notEmpty_;
  std::condition_variable notFull_;
  std::deque<T> q_;
  std::size_t highWater_ = 0;
  bool closed_ = false;
  bool aborted_ = false;
};

}  // namespace pipeline
