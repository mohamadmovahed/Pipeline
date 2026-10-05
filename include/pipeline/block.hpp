// SPDX-License-Identifier: MIT
// pipeline — the base classes you derive your processing blocks from.
#pragma once

#include "parameters.hpp"

#include <atomic>
#include <cstddef>
#include <string>
#include <utility>
#include <vector>

namespace pipeline {

class Stage;

/// Collects the outputs a block produces for one input. A block may emit any
/// number of items per input (0 for decimation / dropped frames, many for
/// interpolation / segmentation); the stage forwards them downstream.
template <typename T>
class Emitter {
 public:
  void emit(T&& v) { buf_.push_back(std::move(v)); }
  void emit(const T& v) { buf_.push_back(v); }
  template <typename... A>
  T& emplace(A&&... a) {
    return buf_.emplace_back(std::forward<A>(a)...);
  }
  std::size_t size() const { return buf_.size(); }
  bool empty() const { return buf_.empty(); }

  /// Hands the collected items to the stage (internal use).
  std::vector<T> take() { return std::move(buf_); }

 private:
  std::vector<T> buf_;
};

/// Common part of every block: parameters and lifecycle hooks.
class BlockBase {
 public:
  BlockBase() = default;
  BlockBase(const BlockBase&) = delete;
  BlockBase& operator=(const BlockBase&) = delete;
  virtual ~BlockBase() = default;

  /// Called on the worker thread before the first item.
  virtual void onStart() {}
  /// Called on the worker thread after the last item (and after flush).
  virtual void onStop() {}

  ParameterSet& parameters() { return params_; }
  const ParameterSet& parameters() const { return params_; }

  /// Index of this replica inside its stage (0 .. workers-1). Unique per
  /// stage, never reused, so it can seed per-replica state.
  std::size_t replicaIndex() const { return replica_; }

  /// True once the pipeline asked to stop. Sources that block waiting for
  /// hardware should poll this.
  bool stopRequested() const { return stop_ && stop_->load(std::memory_order_relaxed); }

 protected:
  /// Declares a control parameter (UI -> block). Call from the constructor or
  /// a member initializer:
  ///   Control<int>& iterations_ = control<int>("iterations", 8, "LDPC iterations", {1, 50});
  template <typename T>
  Control<T>& control(const std::string& name, T initial, const std::string& description = {}, Range range = {},
                      const std::string& unit = {}) {
    return params_.addControl<T>(name, initial, description, range, unit);
  }

  /// Declares a monitor parameter (block -> UI).
  ///   Monitor<double>& snr_ = monitor<double>("snr", "Estimated SNR", Aggregate::Mean, "dB");
  template <typename T>
  Monitor<T>& monitor(const std::string& name, const std::string& description = {}, Aggregate agg = Aggregate::Mean,
                      const std::string& unit = {}, T initial = T{}) {
    return params_.addMonitor<T>(name, description, agg, unit, initial);
  }

 private:
  friend class Stage;
  ParameterSet params_;
  std::size_t replica_ = 0;
  const std::atomic<bool>* stop_ = nullptr;
};

/// A processing block: consumes In, produces zero or more Out per input.
template <typename In, typename Out>
class Block : public BlockBase {
 public:
  using input_type = In;
  using output_type = Out;

  /// Processes one input. Runs on a worker thread. When the stage has several
  /// workers each worker owns its own Block instance, so members need no lock.
  virtual void process(In&& in, Emitter<Out>& out) = 0;

  /// End of stream (or worker retirement): emit anything still buffered.
  virtual void flush(Emitter<Out>& /*out*/) {}
};

/// The start of a chain (ADC, file, socket, ...).
template <typename Out>
class Source : public BlockBase {
 public:
  using output_type = Out;

  /// Produces the next chunk of data. Return false at end of stream.
  virtual bool produce(Emitter<Out>& out) = 0;
};

/// The end of a chain (application, file writer, network, ...).
template <typename In>
class Sink : public BlockBase {
 public:
  using input_type = In;

  virtual void consume(In&& in) = 0;
  virtual void flush() {}
};

}  // namespace pipeline
