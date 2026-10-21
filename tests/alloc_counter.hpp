// SPDX-License-Identifier: MIT
// Test helper: counts calls of the global operator new (see alloc_counter.cpp).
#pragma once

#include <cstdint>

/// Number of global operator new / new[] calls since program start (all threads).
std::uint64_t allocationCount();
