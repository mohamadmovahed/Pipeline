// SPDX-License-Identifier: MIT
// pipeline — bounded blocking queue connecting two stages.
//
// The bound is what makes rate matching automatic: a fast producer blocks when
// the queue in front of a slower consumer is full (backpressure), so the whole
// chain settles at the throughput of its slowest stage without dropping data
// and without unbounded memory growth.
#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

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

namespace detail {

/// FIFO on a circular buffer of slots that never shrinks. It grows by doubling
/// (up to `limit` slots) only while it is full, so once a queue has reached
/// its high-water mark, push and pop never allocate again. Not thread safe:
/// Channel guards it with its mutex.
template <typename T>
class RingQueue {
 public:
  /// An empty queue without storage (used to take over another queue's items).
  RingQueue() = default;
  /// Preallocates min(limit, 16) slots.
  explicit RingQueue(std::size_t limit) : limit_(limit ? limit : 1) {
    slots_.resize((std::min)(limit_, kInitialSlots));
  }

  bool empty() const { return count_ == 0; }
  std::size_t size() const { return count_; }
  /// Number of slots allocated so far (<= limit).
  std::size_t allocated() const { return slots_.size(); }

  /// Appends an item. The caller guarantees size() < limit.
  void push_back(T&& v) {
    if (count_ == slots_.size()) grow();
    slots_[index(count_)].emplace(std::move(v));
    ++count_;
  }

  T& front() { return *slots_[head_]; }

  void pop_front() {
    slots_[head_].reset();
    head_ = index(1);
    --count_;
  }

  /// Exchanges the contents (and the storage) with `other`.
  void swap(RingQueue& other) noexcept {
    slots_.swap(other.slots_);
    std::swap(head_, other.head_);
    std::swap(count_, other.count_);
    std::swap(limit_, other.limit_);
  }

 private:
  static constexpr std::size_t kInitialSlots = 16;

  std::size_t index(std::size_t offset) const { return (head_ + offset) % slots_.size(); }

  void grow() {
    const std::size_t want = slots_.empty() ? 1 : 2 * slots_.size();
    std::vector<std::optional<T>> bigger((std::max)(count_ + 1, (std::min)(limit_, want)));
    for (std::size_t i = 0; i < count_; ++i) bigger[i].emplace(std::move(*slots_[index(i)]));
    slots_.swap(bigger);
    head_ = 0;
  }

  std::vector<std::optional<T>> slots_;
  std::size_t head_ = 0;
  std::size_t count_ = 0;
  std::size_t limit_ = 1;
};

}  // namespace detail

template <typename T>
class Channel {
 public:
  explicit Channel(std::size_t capacity) : capacity_(capacity ? capacity : 1), q_(capacity_) {}
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
    detail::RingQueue<T> drop;  // takes the items; destroyed outside the lock
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
  /// Slots allocated by the queue so far (grows by doubling up to capacity()).
  std::size_t allocatedSlots() const {
    std::lock_guard<std::mutex> lk(m_);
    return q_.allocated();
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
  detail::RingQueue<T> q_;  // circular buffer: no allocation per push
  std::size_t highWater_ = 0;
  bool closed_ = false;
  bool aborted_ = false;
};

}  // namespace pipeline
