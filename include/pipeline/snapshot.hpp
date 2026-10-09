// SPDX-License-Identifier: MIT
// pipeline — plain data describing the state of a running pipeline, meant to
// be shown in a user interface (directly or as JSON, see json.hpp).
#pragma once

#include "parameters.hpp"
#include "value.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace pipeline {

enum class StageKind { Source, Block, Sink };

inline const char* toString(StageKind k) {
  switch (k) {
    case StageKind::Source: return "source";
    case StageKind::Block: return "block";
    case StageKind::Sink: return "sink";
  }
  return "unknown";
}

enum class PipelineState { Idle, Running, Stopping, Finished, Aborted };

inline const char* toString(PipelineState s) {
  switch (s) {
    case PipelineState::Idle: return "idle";
    case PipelineState::Running: return "running";
    case PipelineState::Stopping: return "stopping";
    case PipelineState::Finished: return "finished";
    case PipelineState::Aborted: return "aborted";
  }
  return "unknown";
}

struct ParamSnapshot {
  std::string name;
  std::string description;
  std::string unit;
  ValueType type = ValueType::Double;
  Value value;
  std::optional<double> min;  // controls only
  std::optional<double> max;  // controls only
  Aggregate aggregate = Aggregate::First;  // monitors only
};

struct StageSnapshot {
  std::string name;
  StageKind kind = StageKind::Block;
  std::size_t workers = 0;     // active workers
  std::size_t minWorkers = 0;  // autoscaler lower bound
  std::size_t maxWorkers = 0;  // autoscaler upper bound
  std::uint64_t itemsIn = 0;   // for sources: items produced
  std::uint64_t itemsOut = 0;
  std::uint64_t errors = 0;
  double inRate = 0;        // items / s
  double outRate = 0;       // items / s
  double utilization = 0;   // 0..1, fraction of time workers spend in process()
  double backpressure = 0;  // 0..1, fraction of time workers wait on downstream
  std::size_t queueSize = 0;      // input queue fill (0 for sources)
  std::size_t queueCapacity = 0;
  std::string lastError;
  bool finished = false;
  std::vector<ParamSnapshot> controls;
  std::vector<ParamSnapshot> monitors;
};

struct PipelineSnapshot {
  std::string name;
  PipelineState state = PipelineState::Idle;
  double uptime = 0;       // seconds
  std::string bottleneck;  // busiest non-source stage
  std::string error;       // fatal error that aborted the pipeline, if any
  std::vector<StageSnapshot> stages;

  const StageSnapshot* stage(const std::string& n) const {
    for (auto& s : stages)
      if (s.name == n) return &s;
    return nullptr;
  }
};

}  // namespace pipeline
