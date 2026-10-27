// SPDX-License-Identifier: MIT
// Replaces the global operator new/delete to count allocations, so the tests can
// check that the steady-state path of a running pipeline does not allocate.
// Kept in its own translation unit so the replacements are never inlined into
// callers (GCC would otherwise warn about malloc/free versus new/delete).
#include "alloc_counter.hpp"

#include <atomic>
#include <cstdlib>
#include <new>

namespace {
std::atomic<std::uint64_t> g_allocations{0};
}  // namespace

std::uint64_t allocationCount() { return g_allocations.load(std::memory_order_relaxed); }

void* operator new(std::size_t n) {
  g_allocations.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }
