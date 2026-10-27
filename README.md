# pipeline

A small, header-only **C++17** library for building **pipelined, multi-threaded
processing chains** such as a radio receiver:

```
adc ──▶ demod ──▶ decoder ×N ──▶ datalink ──▶ app
```

* **Pipelined**: every stage runs on its own thread(s), connected by bounded queues.
* **Rates handled automatically**: full queues block the producer (backpressure), so the
  chain runs at the speed of its slowest stage. Blocks may emit 0, 1 or N outputs per input.
* **Parallel stages**: run a slow stage (e.g. the decoder) on several threads with
  `opts().parallel(4)`; results keep their input order.
* **Autoscaling**: `opts().autoscale(1, 8)` adds workers to the bottleneck stage at run time
  and removes them when they are no longer needed.
* **Controls and monitors**: blocks declare typed, range-checked parameters that a UI can
  change while running, and values it can display. `snapshot()` / `snapshotJson()` give the
  whole state (rates, utilization, queue fill, workers, parameters) for any UI toolkit.
* **Receivers on demand**: `rx.input<T>()` gives a `Feeder` to push data into a running
  pipeline; `PipelinePool` starts and stops pipelines by id without blocking the caller,
  e.g. one receiver chain per active channel of a wideband capture.
* **Wrap existing code**: lambdas (`map`, `blockFn`, `sourceFn`, `sinkFn`) turn library calls into stages.
* **No allocation per item in steady state** for single-worker stages (and unordered
  parallel ones): each worker reuses its `Emitter` buffer, a stage that cannot have more
  than one worker skips the reorder buffer, and queues are circular buffers that only grow
  (by doubling, up to their capacity) until they reach their high-water mark. Ordered
  parallel stages still allocate one reorder entry per item. Items are moved, never copied.

**Tutorial:** open [`docs/tutorial.html`](docs/tutorial.html) in a browser.

## Quick look

```cpp
#include <pipeline/pipeline.hpp>
using namespace pipeline;

class Decoder : public Block<SoftFrame, DecodedFrame> {
 public:
  void process(SoftFrame&& in, Emitter<DecodedFrame>& out) override {
    out.emit(decode(in, iterations_.get()));
    ++frames_;
  }
 private:
  Control<int>& iterations_ = control<int>("iterations", 8, "LDPC iterations", {1, 50});
  Monitor<std::int64_t>& frames_ = monitor<std::int64_t>("frames", "frames decoded", Aggregate::Sum);
};

Pipeline rx("receiver");
auto iq   = rx.source<AdcSource>("adc", "/dev/sdr0");
auto soft = rx.add<Demodulator>("demod", iq);
auto bits = rx.add<Decoder>("decoder", soft, opts().autoscale(1, 8));
auto pkts = rx.add<DataLink>("datalink", bits);
rx.sink<AppSink>("app", pkts);

rx.start();
rx.setControl("decoder.iterations", 12);   // from the UI, any thread
std::string state = rx.snapshotJson();     // for the UI
rx.stop();                                 // drain, flush, join
```

## Use it from CMake

```cmake
add_subdirectory(third_party/pipeline)          # or FetchContent / find_package(pipeline)
target_link_libraries(my_receiver PRIVATE pipeline::pipeline)
```

`cmake --install` installs the headers and a `pipelineConfig.cmake` package, so
`find_package(pipeline 1.0 REQUIRED)` works too.

## Build the examples and tests

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/examples/receiver_chain --seconds 9     # live dashboard of a simulated receiver
./build/examples/multichannel_receiver           # one receiver started per active channel
./build/examples/quickstart
```

| Path | Content |
|------|---------|
| `include/pipeline/` | the library (`pipeline.hpp` includes everything) |
| `examples/receiver_chain.cpp` | TX simulator → demodulator → autoscaled decoder → data link → app, with a console UI |
| `examples/multichannel_receiver.cpp` | wideband front end that starts/stops one receiver per active channel (DSP stubbed) |
| `examples/quickstart.cpp` | smallest useful program |
| `tests/test_pipeline.cpp` | dependency-free tests (ordering, rates, controls, autoscaling, errors, shutdown, feeders, pool, queue ring buffer, allocation-free steady state) |
| `docs/tutorial.html` | the tutorial |

Requires a C++17 compiler and CMake 3.16+. No other dependencies. MIT licence.
Changes are listed in [`CHANGELOG.md`](CHANGELOG.md).
