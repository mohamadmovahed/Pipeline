// SPDX-License-Identifier: MIT
// pipeline — control and monitor parameters.
//
//  * Control<T>: written by the UI, read by the block. A UI write is staged and
//    applied by the block's own worker thread *between* two process() calls,
//    so the block never needs a lock and sees a stable value for a whole item.
//  * Monitor<T>: written by the block, read by the UI. Lock free for
//    arithmetic types.
#pragma once

#include "value.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace pipeline {

/// Optional numeric limits of a control. Enforced on every UI write.
struct Range {
  std::optional<double> min;
  std::optional<double> max;
};

/// How monitor values of the parallel replicas of a block are combined into
/// one value for the UI.
enum class Aggregate { Sum, Mean, Min, Max, First };

inline const char* toString(Aggregate a) {
  switch (a) {
    case Aggregate::Sum: return "sum";
    case Aggregate::Mean: return "mean";
    case Aggregate::Min: return "min";
    case Aggregate::Max: return "max";
    case Aggregate::First: return "first";
  }
  return "unknown";
}

class ParameterBase {
 public:
  ParameterBase(std::string name, std::string description, std::string unit, ValueType type)
      : name_(std::move(name)), description_(std::move(description)), unit_(std::move(unit)), type_(type) {}
  ParameterBase(const ParameterBase&) = delete;
  ParameterBase& operator=(const ParameterBase&) = delete;
  virtual ~ParameterBase() = default;

  const std::string& name() const { return name_; }
  const std::string& description() const { return description_; }
  const std::string& unit() const { return unit_; }
  ValueType type() const { return type_; }

  /// Current value, safe to call from any thread.
  virtual Value value() const = 0;

 private:
  std::string name_;
  std::string description_;
  std::string unit_;
  ValueType type_;
};

class ControlBase : public ParameterBase {
 public:
  ControlBase(std::string name, std::string description, std::string unit, ValueType type, Range range)
      : ParameterBase(std::move(name), std::move(description), std::move(unit), type), range_(range) {}

  const Range& range() const { return range_; }

  /// Stages a new value (any thread). Throws std::invalid_argument for a value
  /// that cannot be converted and std::out_of_range for one outside range().
  virtual void request(const Value& v) = 0;

  /// Applies a staged value. Called by the owning worker thread only.
  virtual bool applyPending() = 0;

 private:
  Range range_;
};

template <typename T>
class Control final : public ControlBase {
 public:
  Control(std::string name, T initial, std::string description, Range range, std::string unit,
          std::atomic<bool>* setDirty)
      : ControlBase(std::move(name), std::move(description), std::move(unit), valueTypeOf<T>(), range),
        value_(initial),
        pending_(initial),
        published_(initial),
        setDirty_(setDirty) {
    check(initial);
  }

  /// Value used for processing. Call from the block's own thread (process,
  /// flush, onStart...). It only changes between two process() calls.
  const T& get() const { return value_; }
  operator const T&() const { return value_; }

  /// Sets the value from inside the block (e.g. an adaptive loop). The new
  /// value is visible to the UI immediately.
  void set(const T& v) {
    check(v);
    value_ = v;
    std::lock_guard<std::mutex> lk(m_);
    published_ = v;
  }

  /// Registers a callback invoked on the block's thread whenever a UI change
  /// is applied. Typical use: recompute filter taps when "bandwidth" changes.
  Control& onChange(std::function<void(const T&)> cb) {
    onChange_ = std::move(cb);
    return *this;
  }

  void request(const Value& v) override {
    T t = fromValue<T>(v);
    check(t);
    {
      std::lock_guard<std::mutex> lk(m_);
      pending_ = t;
      published_ = t;
    }
    dirty_.store(true, std::memory_order_release);
    if (setDirty_) setDirty_->store(true, std::memory_order_release);
  }

  bool applyPending() override {
    if (!dirty_.exchange(false, std::memory_order_acq_rel)) return false;
    {
      std::lock_guard<std::mutex> lk(m_);
      value_ = pending_;
    }
    if (onChange_) onChange_(value_);
    return true;
  }

  Value value() const override {
    std::lock_guard<std::mutex> lk(m_);
    return toValue(published_);
  }

 private:
  void check(const T& v) const {
    if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
      const double d = static_cast<double>(v);
      if ((range().min && d < *range().min) || (range().max && d > *range().max)) {
        throw std::out_of_range("control '" + name() + "': value " + toString(toValue(v)) + " is outside [" +
                                (range().min ? toString(Value(*range().min)) : std::string("-inf")) + ", " +
                                (range().max ? toString(Value(*range().max)) : std::string("inf")) + "]");
      }
    }
  }

  T value_;                 // worker thread only
  mutable std::mutex m_;    // guards pending_ and published_
  T pending_;
  T published_;
  std::atomic<bool> dirty_{false};
  std::atomic<bool>* setDirty_;
  std::function<void(const T&)> onChange_;
};

class MonitorBase : public ParameterBase {
 public:
  MonitorBase(std::string name, std::string description, std::string unit, ValueType type, Aggregate agg)
      : ParameterBase(std::move(name), std::move(description), std::move(unit), type), aggregate_(agg) {}
  Aggregate aggregate() const { return aggregate_; }

 private:
  Aggregate aggregate_;
};

template <typename T>
class Monitor final : public MonitorBase {
  static constexpr bool kAtomic = std::is_arithmetic_v<T>;
  struct AtomicStore {
    explicit AtomicStore(T init) : v(init) {}
    std::atomic<T> v;
  };
  struct LockedStore {
    explicit LockedStore(T init) : v(std::move(init)) {}
    mutable std::mutex m;
    T v;
  };

 public:
  Monitor(std::string name, std::string description, Aggregate agg, std::string unit, T initial = T{})
      : MonitorBase(std::move(name), std::move(description), std::move(unit), valueTypeOf<T>(), agg), s_(initial) {}

  void set(const T& v) {
    if constexpr (kAtomic) {
      s_.v.store(v, std::memory_order_relaxed);
    } else {
      std::lock_guard<std::mutex> lk(s_.m);
      s_.v = v;
    }
  }
  Monitor& operator=(const T& v) {
    set(v);
    return *this;
  }

  /// Atomically adds to a numeric monitor (counters).
  void add(const T& d) {
    static_assert(kAtomic && !std::is_same_v<T, bool>, "add() needs a numeric monitor");
    if constexpr (std::is_integral_v<T>) {
      s_.v.fetch_add(d, std::memory_order_relaxed);
    } else {
      T cur = s_.v.load(std::memory_order_relaxed);
      while (!s_.v.compare_exchange_weak(cur, cur + d, std::memory_order_relaxed)) {
      }
    }
  }
  Monitor& operator+=(const T& d) {
    add(d);
    return *this;
  }
  Monitor& operator++() {
    add(T(1));
    return *this;
  }

  T get() const {
    if constexpr (kAtomic) {
      return s_.v.load(std::memory_order_relaxed);
    } else {
      std::lock_guard<std::mutex> lk(s_.m);
      return s_.v;
    }
  }

  Value value() const override { return toValue(get()); }

 private:
  std::conditional_t<kAtomic, AtomicStore, LockedStore> s_;
};

/// The parameters declared by one block instance.
class ParameterSet {
 public:
  ParameterSet() = default;
  ParameterSet(const ParameterSet&) = delete;
  ParameterSet& operator=(const ParameterSet&) = delete;

  template <typename T>
  Control<T>& addControl(const std::string& name, T initial, const std::string& description, Range range,
                         const std::string& unit) {
    checkName(name);
    auto c = std::make_unique<Control<T>>(name, initial, description, range, unit, &anyDirty_);
    auto& ref = *c;
    controls_.push_back(std::move(c));
    return ref;
  }

  template <typename T>
  Monitor<T>& addMonitor(const std::string& name, const std::string& description, Aggregate agg,
                         const std::string& unit, T initial) {
    checkName(name);
    auto m = std::make_unique<Monitor<T>>(name, description, agg, unit, initial);
    auto& ref = *m;
    monitors_.push_back(std::move(m));
    return ref;
  }

  ControlBase* findControl(const std::string& name) const {
    for (auto& c : controls_)
      if (c->name() == name) return c.get();
    return nullptr;
  }
  MonitorBase* findMonitor(const std::string& name) const {
    for (auto& m : monitors_)
      if (m->name() == name) return m.get();
    return nullptr;
  }

  const std::vector<std::unique_ptr<ControlBase>>& controls() const { return controls_; }
  const std::vector<std::unique_ptr<MonitorBase>>& monitors() const { return monitors_; }

  /// Applies staged UI writes. Cheap (one atomic) when nothing changed.
  void applyPending() {
    if (!anyDirty_.exchange(false, std::memory_order_acq_rel)) return;
    for (auto& c : controls_) c->applyPending();
  }

 private:
  void checkName(const std::string& name) const {
    if (name.empty()) throw std::invalid_argument("parameter name must not be empty");
    if (findControl(name) || findMonitor(name))
      throw std::invalid_argument("duplicate parameter name '" + name + "'");
  }

  std::atomic<bool> anyDirty_{false};
  std::vector<std::unique_ptr<ControlBase>> controls_;
  std::vector<std::unique_ptr<MonitorBase>> monitors_;
};

}  // namespace pipeline
