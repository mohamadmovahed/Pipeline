// SPDX-License-Identifier: MIT
// pipeline — JSON serialisation of snapshots, for web / remote user interfaces.
#pragma once

#include "snapshot.hpp"
#include "value.hpp"

#include <cmath>
#include <cstdio>
#include <optional>
#include <string>

namespace pipeline {

inline std::string jsonEscape(const std::string& s) {
  std::string o;
  o.reserve(s.size() + 2);
  o += '"';
  for (unsigned char c : s) {
    switch (c) {
      case '"': o += "\\\""; break;
      case '\\': o += "\\\\"; break;
      case '\n': o += "\\n"; break;
      case '\r': o += "\\r"; break;
      case '\t': o += "\\t"; break;
      default:
        if (c < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof buf, "\\u%04x", c);
          o += buf;
        } else {
          o += static_cast<char>(c);
        }
    }
  }
  o += '"';
  return o;
}

inline std::string jsonNumber(double d) {
  if (!std::isfinite(d)) return "null";
  char buf[32];
  std::snprintf(buf, sizeof buf, "%.10g", d);
  return buf;
}

inline std::string toJson(const Value& v) {
  switch (typeOf(v)) {
    case ValueType::Bool: return std::get<bool>(v) ? "true" : "false";
    case ValueType::Int: return std::to_string(std::get<std::int64_t>(v));
    case ValueType::Double: return jsonNumber(std::get<double>(v));
    case ValueType::String: return jsonEscape(std::get<std::string>(v));
  }
  return "null";
}

inline std::string toJson(const ParamSnapshot& p, bool control) {
  std::string o = "{\"name\":" + jsonEscape(p.name) + ",\"type\":\"" + toString(p.type) +
                  "\",\"value\":" + toJson(p.value);
  if (!p.description.empty()) o += ",\"description\":" + jsonEscape(p.description);
  if (!p.unit.empty()) o += ",\"unit\":" + jsonEscape(p.unit);
  if (control) {
    if (p.min) o += ",\"min\":" + jsonNumber(*p.min);
    if (p.max) o += ",\"max\":" + jsonNumber(*p.max);
  } else {
    o += std::string(",\"aggregate\":\"") + toString(p.aggregate) + "\"";
  }
  return o + "}";
}

inline std::string toJson(const StageSnapshot& s) {
  std::string o = "{\"name\":" + jsonEscape(s.name) + ",\"kind\":\"" + toString(s.kind) + "\"";
  o += ",\"workers\":" + std::to_string(s.workers);
  o += ",\"minWorkers\":" + std::to_string(s.minWorkers);
  o += ",\"maxWorkers\":" + std::to_string(s.maxWorkers);
  o += ",\"itemsIn\":" + std::to_string(s.itemsIn);
  o += ",\"itemsOut\":" + std::to_string(s.itemsOut);
  o += ",\"errors\":" + std::to_string(s.errors);
  o += ",\"inRate\":" + jsonNumber(s.inRate);
  o += ",\"outRate\":" + jsonNumber(s.outRate);
  o += ",\"utilization\":" + jsonNumber(s.utilization);
  o += ",\"backpressure\":" + jsonNumber(s.backpressure);
  o += ",\"queueSize\":" + std::to_string(s.queueSize);
  o += ",\"queueCapacity\":" + std::to_string(s.queueCapacity);
  o += std::string(",\"finished\":") + (s.finished ? "true" : "false");
  if (!s.lastError.empty()) o += ",\"lastError\":" + jsonEscape(s.lastError);
  o += ",\"controls\":[";
  for (std::size_t i = 0; i < s.controls.size(); ++i) o += (i ? "," : "") + toJson(s.controls[i], true);
  o += "],\"monitors\":[";
  for (std::size_t i = 0; i < s.monitors.size(); ++i) o += (i ? "," : "") + toJson(s.monitors[i], false);
  return o + "]}";
}

inline std::string toJson(const PipelineSnapshot& p) {
  std::string o = "{\"name\":" + jsonEscape(p.name) + ",\"state\":\"" + toString(p.state) + "\"";
  o += ",\"uptime\":" + jsonNumber(p.uptime);
  o += ",\"bottleneck\":" + jsonEscape(p.bottleneck);
  if (!p.error.empty()) o += ",\"error\":" + jsonEscape(p.error);
  o += ",\"stages\":[";
  for (std::size_t i = 0; i < p.stages.size(); ++i) o += (i ? "," : "") + toJson(p.stages[i]);
  return o + "]}";
}

}  // namespace pipeline
