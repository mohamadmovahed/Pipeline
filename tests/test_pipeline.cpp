// Tests for the pipeline library. Dependency free: run the binary or `ctest`.
#include <pipeline/pipeline.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace pipeline;
using namespace std::chrono_literals;

namespace {

int g_failures = 0;

#define CHECK(cond)                                                                \
  do {                                                                             \
    if (!(cond)) {                                                                 \
      std::cerr << "  CHECK failed: " #cond " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
      ++g_failures;                                                                \
    }                                                                              \
  } while (0)

template <typename E, typename F>
bool throws(F&& f) {
  try {
    f();
  } catch (const E&) {
    return true;
  } catch (...) {
    return false;
  }
  return false;
}

// Emits 0 .. n-1 then ends.
class Counter : public Source<int> {
 public:
  explicit Counter(int n) : n_(n) {}
  bool produce(Emitter<int>& out) override {
    if (i_ >= n_) return false;
    out.emit(i_++);
    return i_ < n_;
  }

 private:
  int n_;
  int i_ = 0;
};

// Emits forever until stop is requested.
class Endless : public Source<int> {
 public:
  bool produce(Emitter<int>& out) override {
    out.emit(i_++);
    return true;
  }

 private:
  int i_ = 0;
};

// Doubles its input after a random delay; used to scramble completion order.
class SlowDouble : public Block<int, int> {
 public:
  bool dummy = false;
  void process(int&& x, Emitter<int>& out) override {
    std::uniform_int_distribution<int> d(0, 300);
    std::this_thread::sleep_for(std::chrono::microseconds(d(rng_)));
    ++processed_;
    out.emit(2 * x);
  }

 private:
  std::mt19937 rng_{static_cast<unsigned>(replicaIndex() + 1)};
  Monitor<std::int64_t>& processed_ = monitor<std::int64_t>("processed", "items", Aggregate::Sum);
};

// Sums groups of `n` inputs: a 1/n rate block. Flush emits a partial group.
class Decimate : public Block<int, int> {
 public:
  void process(int&& x, Emitter<int>& out) override {
    acc_ += x;
    if (++count_ == factor_.get()) {
      out.emit(acc_);
      acc_ = count_ = 0;
    }
  }
  void flush(Emitter<int>& out) override {
    if (count_) out.emit(acc_);
    acc_ = count_ = 0;
  }

 private:
  Control<int>& factor_ = control<int>("factor", 4, "decimation factor", {1, 100});
  int acc_ = 0;
  int count_ = 0;
};

class Collect : public Sink<int> {
 public:
  explicit Collect(std::vector<int>* v) : v_(v) {}
  void consume(int&& x) override { v_->push_back(x); }

 private:
  std::vector<int>* v_;
};

void testOrderedParallel() {
  std::vector<int> got;
  Pipeline p("ordered");
  auto src = p.source<Counter>("src", 2000);
  auto dbl = p.add<SlowDouble>("double", src, opts().parallel(6));
  p.sink<Collect>("sink", dbl, opts(), &got);
  p.start();
  p.wait();
  CHECK(p.state() == PipelineState::Finished);
  CHECK(got.size() == 2000);
  bool inOrder = true;
  for (std::size_t i = 0; i < got.size(); ++i) inOrder = inOrder && got[i] == static_cast<int>(2 * i);
  CHECK(inOrder);
  CHECK(std::get<std::int64_t>(p.getMonitor("double.processed")) == 2000);  // Sum over 6 replicas
  auto snap = p.snapshot();
  CHECK(snap.stage("double")->workers == 6);
  CHECK(snap.stage("double")->itemsIn == 2000);
  CHECK(snap.stage("sink")->finished);
}

void testUnorderedParallel() {
  std::vector<int> got;
  Pipeline p("unordered");
  auto src = p.source<Counter>("src", 1000);
  auto dbl = p.add<SlowDouble>("double", src, opts().parallel(4).unordered());
  p.sink<Collect>("sink", dbl, opts(), &got);
  p.start();
  p.wait();
  CHECK(got.size() == 1000);
  std::int64_t sum = 0;
  for (int v : got) sum += v;
  CHECK(sum == 2LL * 999 * 1000 / 2);
}

void testRateChangeAndFlush() {
  std::vector<int> got;
  Pipeline p("rates");
  auto src = p.source<Counter>("src", 10);  // 0..9
  auto dec = p.add<Decimate>("dec", src);
  p.sink<Collect>("sink", dec, opts(), &got);
  p.start();
  p.wait();
  // groups of 4: 0+1+2+3, 4+5+6+7, then flush 8+9
  CHECK((got == std::vector<int>{6, 22, 17}));
}

void testControls() {
  std::vector<int> got;
  Pipeline p("controls");
  auto src = p.source<Counter>("src", 9);
  auto dec = p.add<Decimate>("dec", src);
  p.sink<Collect>("sink", dec, opts(), &got);

  CHECK(std::get<std::int64_t>(p.getControl("dec.factor")) == 4);
  p.setControl("dec.factor", 3);  // before start: applied to the first item
  CHECK(std::get<std::int64_t>(p.getControl("dec.factor")) == 3);
  CHECK(throws<std::out_of_range>([&] { p.setControl("dec.factor", 1000); }));
  CHECK(throws<std::invalid_argument>([&] { p.setControl("dec.nope", 1); }));
  CHECK(throws<std::invalid_argument>([&] { p.setControl("nope.factor", 1); }));
  CHECK(throws<std::invalid_argument>([&] { p.setControlFromString("dec.factor", "abc"); }));
  p.setControlFromString("dec.factor", "3");
  CHECK(std::get<std::int64_t>(p.getControl("dec.factor")) == 3);

  auto snap = p.snapshot();
  const auto* st = snap.stage("dec");
  CHECK(st && st->controls.size() == 1 && st->controls[0].max && *st->controls[0].max == 100);

  p.start();
  p.wait();
  CHECK((got == std::vector<int>{3, 12, 21}));
}

void testControlsWhileRunning() {
  struct Probe : Block<int, int> {
    Control<double>& gain = control<double>("gain", 1.0, "", {0.0, 10.0});
    Monitor<double>& seen = monitor<double>("seen_gain");
    int changes = 0;
    Probe() {
      gain.onChange([this](const double&) { ++changes; });
    }
    void process(int&& x, Emitter<int>& out) override {
      seen = gain.get();
      out.emit(static_cast<int>(x * gain.get()));
    }
  };
  Pipeline p("live");
  auto src = p.source<Endless>("src");
  auto pr = p.add<Probe>("probe", src);
  p.sinkFn("sink", pr, [](int&&) { std::this_thread::sleep_for(50us); });
  p.start();
  std::this_thread::sleep_for(50ms);
  p.setControl("probe.gain", 2.5);
  bool applied = false;
  for (int i = 0; i < 200 && !applied; ++i) {
    std::this_thread::sleep_for(5ms);
    applied = std::get<double>(p.getMonitor("probe.seen_gain")) == 2.5;
  }
  CHECK(applied);
  p.stop();
  CHECK(p.state() == PipelineState::Finished);
}

void testLambdas() {
  std::vector<std::string> got;
  Pipeline p("lambdas");
  int n = 0;
  auto src = p.sourceFn<int>("gen", [n](Emitter<int>& e) mutable {
    e.emit(n++);
    return n < 5;
  });
  auto sq = p.map("square", src, [](int&& x) { return x * x; });
  auto txt = p.blockFn<std::string>("text", sq, [](int&& x, Emitter<std::string>& e) {
    if (x % 2 == 0) e.emit("v" + std::to_string(x));  // drops odd values
  });
  p.sinkFn("sink", txt, [&got](std::string&& s) { got.push_back(std::move(s)); });
  p.start();
  p.wait();
  (void)n;
  CHECK((got == std::vector<std::string>{"v0", "v4", "v16"}));
}

void testErrorsSkip() {
  std::vector<int> got;
  Pipeline p("errors");
  auto src = p.source<Counter>("src", 100);
  auto f = p.blockFn<int>("flaky", src, [](int&& x, Emitter<int>& e) {
    if (x % 10 == 3) throw std::runtime_error("bad item " + std::to_string(x));
    e.emit(x);
  }, opts().parallel(3));
  p.sink<Collect>("sink", f, opts(), &got);
  p.start();
  p.wait();
  CHECK(p.state() == PipelineState::Finished);
  CHECK(got.size() == 90);
  auto s = *p.snapshot().stage("flaky");
  CHECK(s.errors == 10);
  CHECK(s.lastError.rfind("bad item", 0) == 0);
}

void testErrorsStop() {
  Pipeline p("fatal");
  auto src = p.source<Endless>("src");
  auto f = p.blockFn<int>("fragile", src, [](int&& x, Emitter<int>& e) {
    if (x == 50) throw std::runtime_error("boom");
    e.emit(x);
  }, opts().onError(ErrorPolicy::Stop));
  p.sinkFn("sink", f, [](int&&) {});
  p.start();
  CHECK(p.waitFor(5s));
  CHECK(p.state() == PipelineState::Aborted);
  CHECK(p.snapshot().error == "fragile: boom");
}

void testAbortAndStop() {
  {
    Pipeline p("abort");
    auto src = p.source<Endless>("src");
    auto slow = p.map("slow", src, [](int&& x) {
      std::this_thread::sleep_for(1ms);
      return x;
    });
    p.sinkFn("sink", slow, [](int&&) {});
    p.start();
    std::this_thread::sleep_for(100ms);
    auto t0 = std::chrono::steady_clock::now();
    p.abort();
    CHECK(std::chrono::steady_clock::now() - t0 < 2s);
    CHECK(p.state() == PipelineState::Aborted);
  }
  {
    // graceful stop: everything produced reaches the sink
    std::atomic<std::int64_t> received{0};
    Pipeline p("stop");
    auto src = p.source<Endless>("src");
    auto m = p.map("m", src, [](int&& x) { return x; }, opts().parallel(3));
    p.sinkFn("sink", m, [&](int&&) { ++received; });
    p.start();
    std::this_thread::sleep_for(100ms);
    p.stop();
    CHECK(p.state() == PipelineState::Finished);
    auto s = p.snapshot();
    CHECK(static_cast<std::int64_t>(s.stage("src")->itemsOut) == received.load());
  }
  {
    // destructor of a running pipeline must not hang or crash
    Pipeline p("dtor");
    auto src = p.source<Endless>("src");
    p.sinkFn("sink", src, [](int&&) {});
    p.start();
    std::this_thread::sleep_for(20ms);
  }
}

void testAutoscale() {
  PipelineOptions po;
  po.monitorPeriod = 50ms;
  po.scaleCooldown = 100ms;
  Pipeline p("autoscale", po);
  auto src = p.source<Endless>("src");
  auto heavy = p.map("heavy", src, [](int&& x) {
    auto until = std::chrono::steady_clock::now() + 2ms;
    while (std::chrono::steady_clock::now() < until) {
    }  // busy CPU work
    return x;
  }, opts().autoscale(1, 4));
  p.sinkFn("sink", heavy, [](int&&) {});
  p.start();
  std::size_t maxSeen = 1;
  for (int i = 0; i < 60; ++i) {
    std::this_thread::sleep_for(50ms);
    maxSeen = std::max(maxSeen, p.stage("heavy").workers());
  }
  auto snap = p.snapshot();
  p.stop();
  CHECK(maxSeen == 4);
  CHECK(snap.bottleneck == "heavy");
}

void testSumSurvivesRetirement() {
  PipelineOptions po;
  po.monitorPeriod = 20ms;
  po.autoscale = false;  // manual scaling only
  Pipeline p("retire", po);
  std::atomic<std::int64_t> received{0};
  auto src = p.source<Endless>("src");
  auto dbl = p.add<SlowDouble>("double", src, opts().autoscale(1, 4));
  p.sinkFn("sink", dbl, [&](int&&) { ++received; });
  p.start();
  CHECK(p.stage("double").setWorkers(4) == 4);
  std::this_thread::sleep_for(150ms);
  CHECK(p.stage("double").setWorkers(1) == 1);
  std::this_thread::sleep_for(200ms);  // retired workers exit and are reaped
  p.stop();
  CHECK(p.snapshot().stage("double")->workers == 1);
  CHECK(std::get<std::int64_t>(p.getMonitor("double.processed")) == received.load());
}

void testBuildErrors() {
  Pipeline p("build");
  auto src = p.source<Counter>("src", 1);
  CHECK(throws<std::invalid_argument>([&] { p.source<Counter>("src", 1); }));     // duplicate name
  CHECK(throws<std::invalid_argument>([&] { p.source<Counter>("a.b", 1); }));     // dot in name
  p.sinkFn("sink", src, [](int&&) {});
  CHECK(throws<std::logic_error>([&] { p.sinkFn("sink2", src, [](int&&) {}); }));  // double connect
  auto dangling = p.source<Counter>("dangling", 1);
  (void)dangling;
  CHECK(throws<std::logic_error>([&] { p.start(); }));  // unconnected output
}

void testJson() {
  Pipeline p("json \"test\"");
  auto src = p.source<Counter>("src", 10);
  auto d = p.add<Decimate>("dec", src);
  p.sinkFn("sink", d, [](int&&) {});
  p.start();
  p.wait();
  std::string j = p.snapshotJson();
  CHECK(j.find("\"name\":\"json \\\"test\\\"\"") != std::string::npos);
  CHECK(j.find("\"name\":\"factor\",\"type\":\"int\",\"value\":4") != std::string::npos);
  CHECK(j.find("\"state\":\"finished\"") != std::string::npos);
}

void testOnUpdate() {
  PipelineOptions po;
  po.monitorPeriod = 20ms;
  Pipeline p("update", po);
  std::atomic<int> calls{0};
  p.onUpdate([&](const PipelineSnapshot& s) {
    if (s.stages.size() == 2) ++calls;
  });
  auto src = p.source<Endless>("src");
  p.sinkFn("sink", src, [](int&&) {});
  p.start();
  std::this_thread::sleep_for(200ms);
  p.stop();
  CHECK(calls.load() >= 3);
}

}  // namespace

int main() {
  const std::vector<std::pair<const char*, std::function<void()>>> tests = {
      {"ordered parallel", testOrderedParallel},
      {"unordered parallel", testUnorderedParallel},
      {"rate change + flush", testRateChangeAndFlush},
      {"controls", testControls},
      {"controls while running", testControlsWhileRunning},
      {"lambda stages", testLambdas},
      {"errors: skip", testErrorsSkip},
      {"errors: stop", testErrorsStop},
      {"abort / stop / dtor", testAbortAndStop},
      {"autoscale", testAutoscale},
      {"sum monitors survive retirement", testSumSurvivesRetirement},
      {"build errors", testBuildErrors},
      {"json", testJson},
      {"onUpdate", testOnUpdate},
  };
  for (auto& [name, fn] : tests) {
    const int before = g_failures;
    auto t0 = std::chrono::steady_clock::now();
    fn();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    std::cout << (g_failures == before ? "[ OK ] " : "[FAIL] ") << name << " (" << ms << " ms)\n";
  }
  std::cout << (g_failures ? "FAILED" : "ALL TESTS PASSED") << "\n";
  return g_failures ? 1 : 0;
}
