# Changelog

All notable changes to this project are documented here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and semantic versioning.

## [1.2.0] - 2026-10-20

No breaking changes; existing code compiles and behaves as before.

### Changed
- `Channel` stores its items in a circular buffer (`detail::RingQueue`) instead of a
  `std::deque`. The buffer starts with min(capacity, 16) slots and doubles, up to the
  capacity, only when it is full; after that, push and pop never allocate. This removes the
  allocation per push of MSVC's `std::deque` for items larger than 8 bytes.
- Every worker keeps one `Emitter` for its whole life and clears it after each call
  instead of building a new one per item, so emitting does not allocate in steady state.
- A stage whose maximum worker count is 1 skips the reorder buffer even when it is ordered
  (it is ordered by construction): no reorder lock and no map node per item.

### Added
- `Emitter::items()` and `Emitter::clear()` (used by the stages; `take()` is kept).
- `Channel::allocatedSlots()`.
- Tests: queue ring buffer (wrap-around, growth, full/close/abort), a two-thread queue
  test, emitter reuse, a counting `operator new` check that a running chain of single-worker
  stages makes no allocation per item, and ordering of a single-worker ordered stage feeding
  an autoscaled one. The tests build with `-Wshadow`.

### Fixed
- `-Wshadow` warnings in `Monitor`'s value stores (`parameters.hpp`).

## [1.1.0]

- Push inputs (`Pipeline::input`, `Feeder`) and `PipelinePool`.

## [1.0.0]

- First release.
