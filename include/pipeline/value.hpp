// SPDX-License-Identifier: MIT
// pipeline — Value: the dynamically typed value used to move control and
// monitor parameters between processing blocks and a user interface.
#pragma once

#include <cmath>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>

namespace pipeline {

/// A parameter value as seen by the UI. Index order matches ValueType.
using Value = std::variant<bool, std::int64_t, double, std::string>;

enum class ValueType { Bool = 0, Int = 1, Double = 2, String = 3 };

inline const char* toString(ValueType t) {
  switch (t) {
    case ValueType::Bool: return "bool";
    case ValueType::Int: return "int";
    case ValueType::Double: return "double";
    case ValueType::String: return "string";
  }
  return "unknown";
}

inline ValueType typeOf(const Value& v) { return static_cast<ValueType>(v.index()); }

namespace detail {
template <typename>
inline constexpr bool always_false = false;
}  // namespace detail

/// Maps a C++ parameter type to its UI ValueType.
template <typename T>
constexpr ValueType valueTypeOf() {
  if constexpr (std::is_same_v<T, bool>) {
    return ValueType::Bool;
  } else if constexpr (std::is_integral_v<T> || std::is_enum_v<T>) {
    return ValueType::Int;
  } else if constexpr (std::is_floating_point_v<T>) {
    return ValueType::Double;
  } else if constexpr (std::is_same_v<T, std::string>) {
    return ValueType::String;
  } else {
    static_assert(detail::always_false<T>,
                  "parameter types must be bool, an integer, an enum, a floating point type or std::string");
  }
}

template <typename T>
Value toValue(const T& v) {
  if constexpr (std::is_same_v<T, bool>) {
    return Value(v);
  } else if constexpr (std::is_enum_v<T>) {
    return Value(static_cast<std::int64_t>(v));
  } else if constexpr (std::is_integral_v<T>) {
    return Value(static_cast<std::int64_t>(v));
  } else if constexpr (std::is_floating_point_v<T>) {
    return Value(static_cast<double>(v));
  } else {
    static_assert(std::is_same_v<T, std::string>, "unsupported parameter type");
    return Value(v);
  }
}

inline std::string toString(const Value& v) {
  switch (typeOf(v)) {
    case ValueType::Bool: return std::get<bool>(v) ? "true" : "false";
    case ValueType::Int: return std::to_string(std::get<std::int64_t>(v));
    case ValueType::Double: {
      std::ostringstream os;
      os.precision(10);
      os << std::get<double>(v);
      return os.str();
    }
    case ValueType::String: return std::get<std::string>(v);
  }
  return {};
}

/// Parses user text (e.g. typed into a UI field) into a Value of the given type.
inline Value parseValue(ValueType type, const std::string& text) {
  try {
    switch (type) {
      case ValueType::Bool:
        if (text == "true" || text == "1" || text == "on" || text == "yes") return Value(true);
        if (text == "false" || text == "0" || text == "off" || text == "no") return Value(false);
        break;
      case ValueType::Int: {
        std::size_t pos = 0;
        long long n = std::stoll(text, &pos, 0);
        if (pos == text.size()) return Value(static_cast<std::int64_t>(n));
        double d = std::stod(text, &pos);  // accept "1e3" for ints
        if (pos == text.size()) return Value(static_cast<std::int64_t>(std::llround(d)));
        break;
      }
      case ValueType::Double: {
        std::size_t pos = 0;
        double d = std::stod(text, &pos);
        if (pos == text.size()) return Value(d);
        break;
      }
      case ValueType::String: return Value(text);
    }
  } catch (const std::logic_error&) {
  }
  throw std::invalid_argument("cannot parse '" + text + "' as " + toString(type));
}

/// Converts a Value to double (bool -> 0/1). Throws for strings.
inline double toDouble(const Value& v) {
  switch (typeOf(v)) {
    case ValueType::Bool: return std::get<bool>(v) ? 1.0 : 0.0;
    case ValueType::Int: return static_cast<double>(std::get<std::int64_t>(v));
    case ValueType::Double: return std::get<double>(v);
    case ValueType::String: break;
  }
  throw std::invalid_argument("string value is not numeric");
}

/// Converts a Value to the C++ type T, with numeric conversions where sensible.
template <typename T>
T fromValue(const Value& v) {
  if constexpr (std::is_same_v<T, std::string>) {
    return toString(v);
  } else {
    if (typeOf(v) == ValueType::String) return fromValue<T>(parseValue(valueTypeOf<T>(), std::get<std::string>(v)));
    double d = toDouble(v);
    if constexpr (std::is_same_v<T, bool>) {
      return d != 0.0;
    } else if constexpr (std::is_integral_v<T> || std::is_enum_v<T>) {
      if (typeOf(v) == ValueType::Int) return static_cast<T>(std::get<std::int64_t>(v));
      return static_cast<T>(std::llround(d));
    } else {
      return static_cast<T>(d);
    }
  }
}

}  // namespace pipeline
