// Tests for the pipeline library. Dependency free: run the binary or `ctest`.
#include <pipeline/pipeline.hpp>

#include "alloc_counter.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
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

// Test blocks live at namespace scope: MSVC fails to look up a function-local
// enum from the default member initializers of a function-local class.
enum class Mode { A = 0, B = 1 };
struct Typed : Block<int, int> {
  Control<int>& i = control<int>("i", 0);
  Control<double>& d = control<double>("d", 0.0);
  Control<bool>& b = control<bool>("b", false);
  Control<std::string>& s = control<std::string>("s", "x");
  Control<Mode>& m = control<Mode>("m", Mode::A);
  void process(int&& x, Emitter<int>& out) override { out.emit(x); }
};

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

// Plain C++ values must not depend on std::variant's converting constructor,
// which is ambiguous for int on MSVC in C++17 mode.
void testControlValueTypes() {
  Pipeline p("types");
  auto src = p.source<Counter>("src", 1);
  auto t = p.add<Typed>("t", src);
  p.sinkFn("sink", t, [](int&&) {});
  short sh = 7;
  p.setControl("t.i", 60);
  CHECK(std::get<std::int64_t>(p.getControl("t.i")) == 60);
  p.setControl("t.i", sh);
  p.setControl("t.i", 5L);
  p.setControl("t.d", 2.5);
  p.setControl("t.d", 3);  // int into a double control
  CHECK(std::get<double>(p.getControl("t.d")) == 3.0);
  p.setControl("t.d", 1.5f);
  p.setControl("t.b", true);
  CHECK(std::get<bool>(p.getControl("t.b")));
  p.setControl("t.s", "hello");
  CHECK(std::get<std::string>(p.getControl("t.s")) == "hello");
  p.setControl("t.s", std::string("world"));
  p.setControl("t.m", Mode::B);
  CHECK(std::get<std::int64_t>(p.getControl("t.m")) == 1);
  p.setControl("t.i", Value(std::int64_t{9}));
  p.stage("t").setControl("i", 11);
  CHECK(std::get<std::int64_t>(p.getControl("t.i")) == 11);
  p.start();
  p.wait();
}

void testControlsWhileRunning() {
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

void testFeederEndOfStream() {
  std::vector<int> got;
  Pipeline p("feeder");
  auto [port, feeder] = p.input<int>("in", 8);
  auto dbl = p.add<SlowDouble>("double", port, opts().parallel(3));
  p.sink<Collect>("sink", dbl, opts(), &got);
  p.start();
  for (int i = 0; i < 500; ++i) CHECK(feeder.push(i));  // blocking push: nothing dropped
  feeder.close();                                      // end of stream
  CHECK(p.waitFor(10s));
  CHECK(p.state() == PipelineState::Finished);
  CHECK(got.size() == 500);
  bool inOrder = true;
  for (std::size_t i = 0; i < got.size(); ++i) inOrder = inOrder && got[i] == static_cast<int>(2 * i);
  CHECK(inOrder);
  CHECK(!feeder.push(1));  // closed
  CHECK(feeder.pushed() == 500 && feeder.dropped() == 0);
}

void testFeederStopDrainsAndTryPushDrops() {
  std::atomic<int> received{0};
  Pipeline p("feeder-stop");
  auto in = p.input<int>("in", 4);
  auto slow = p.map("slow", in.port, [](int&& x) {
    std::this_thread::sleep_for(2ms);
    return x;
  }, opts().queue(2));
  p.sinkFn("sink", slow, [&](int&&) { ++received; });
  p.start();
  int accepted = 0;
  for (int i = 0; i < 200; ++i) accepted += in.feeder.tryPush(i) ? 1 : 0;  // never blocks
  CHECK(in.feeder.dropped() > 0);
  CHECK(static_cast<std::uint64_t>(accepted) + in.feeder.dropped() == 200);
  p.stop();  // graceful: everything accepted is still delivered
  CHECK(received.load() == accepted);
  CHECK(!in.feeder.tryPush(1));
  CHECK(std::get<std::int64_t>(p.getMonitor("in.dropped")) >= 0);
}

std::unique_ptr<Pipeline> makeCountingReceiver(const std::string& id, Feeder<int>& feeder,
                                               std::atomic<int>* received) {
  auto p = std::make_unique<Pipeline>(id);
  auto in = p->input<int>("in", 16);
  auto m = p->map("work", in.port, [](int&& x) { return x + 1; });
  p->sinkFn("sink", m, [received](int&&) { ++*received; });
  feeder = in.feeder;
  return p;
}

void testPool() {
  std::atomic<int> received{0};
  std::mutex fm;
  std::vector<std::string> finishedIds;
  {
    PipelinePool pool("rx", 20ms);
    pool.onFinished([&](const std::string& id, const PipelineSnapshot& last) {
      std::lock_guard<std::mutex> lk(fm);
      finishedIds.push_back(id + ":" + toString(last.state));
    });
    Feeder<int> a, b;
    pool.start("a", makeCountingReceiver("a", a, &received));
    pool.start("b", makeCountingReceiver("b", b, &received));
    Feeder<int> unused;
    CHECK(throws<std::logic_error>([&] { pool.start("a", makeCountingReceiver("a", unused, &received)); }));
    CHECK(!unused.push(1));  // its pipeline was destroyed unstarted: push fails instead of blocking
    CHECK(pool.size() == 2);
    for (int i = 0; i < 100; ++i) {
      CHECK(a.push(i));
      CHECK(b.push(i));
    }
    CHECK(throws<std::invalid_argument>([&] { pool.setControl("a", "in.nope", 1); }));
    CHECK(!pool.setControl("zzz", "in.x", 1));

    CHECK(pool.stop("a"));       // returns at once, drains in the background
    CHECK(!pool.contains("a"));
    Feeder<int> a2;              // id can be reused immediately
    pool.start("a", makeCountingReceiver("a", a2, &received));
    CHECK(a2.push(1));

    b.close();                   // a receiver whose input ends is reaped by itself
    for (int i = 0; i < 100 && pool.contains("b"); ++i) std::this_thread::sleep_for(10ms);
    CHECK(!pool.contains("b"));

    auto snap = pool.snapshot();
    CHECK(snap.active.size() == 1 && snap.active[0].id == "a");
    CHECK(pool.snapshotJson().find("\"id\":\"a\"") != std::string::npos);

    pool.stopAll();
    pool.waitAll();
    CHECK(pool.size() == 0);
    CHECK(received.load() == 201);
    CHECK(pool.snapshot().finished == 3);
  }
  std::lock_guard<std::mutex> lk(fm);
  CHECK(finishedIds.size() == 3);
}

void testPoolDestructorAborts() {
  PipelinePool pool("dtor", 20ms);
  for (int i = 0; i < 5; ++i) {
    auto p = std::make_unique<Pipeline>("p");
    auto src = p->source<Endless>("src");
    p->sinkFn("sink", src, [](int&&) {});
    pool.start("p" + std::to_string(i), std::move(p));
  }
  std::this_thread::sleep_for(30ms);
}  // must not hang

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

void testChannelRing() {
  // FIFO order across many wrap-arounds of a small queue.
  Channel<int> c(5);
  int next = 0, expect = 0;
  bool fifo = true;
  for (int round = 0; round < 1000; ++round) {
    const int n = 1 + round % 5;
    for (int i = 0; i < n; ++i) CHECK(c.tryPush(int(next++)));
    for (int i = 0; i < n; ++i) {
      std::optional<int> v;
      CHECK(c.popFor(v, 0ms) == PopStatus::Item);
      fifo = fifo && v && *v == expect++;
    }
  }
  CHECK(fifo);
  CHECK(c.allocatedSlots() == 5);  // never more than the capacity

  // Full: tryPush fails and leaves the item untouched.
  Channel<std::unique_ptr<int>> f(2);
  CHECK(f.tryPush(std::make_unique<int>(1)));
  CHECK(f.tryPush(std::make_unique<int>(2)));
  auto keep = std::make_unique<int>(3);
  CHECK(!f.tryPush(std::move(keep)));
  CHECK(keep && *keep == 3);
  CHECK(!f.pushFor(std::move(keep), 1ms));
  CHECK(keep && *keep == 3);
  CHECK(*f.pop().value() == 1);  // pop/push wraps the 2-slot ring
  CHECK(f.tryPush(std::move(keep)));
  CHECK(*f.pop().value() == 2);
  CHECK(*f.pop().value() == 3);

  // Large capacity: storage grows by doubling, preserving order, up to the capacity.
  Channel<int> g(100);
  CHECK(g.allocatedSlots() == 16);
  for (int i = 0; i < 10; ++i) g.push(int(i));
  for (int i = 0; i < 10; ++i) CHECK(g.pop().value() == i);  // head is now mid-buffer
  for (int i = 0; i < 100; ++i) CHECK(g.tryPush(int(i)));
  CHECK(!g.tryPush(100));
  CHECK(g.allocatedSlots() == 100);
  CHECK(g.highWater() == 100);
  bool ordered = true;
  for (int i = 0; i < 100; ++i) ordered = ordered && g.pop().value() == i;
  CHECK(ordered);

  // close(): drains, then reports Closed. abort(): destroys queued items.
  Channel<int> cl(4);
  cl.push(7);
  cl.close();
  CHECK(!cl.tryPush(8));
  CHECK(cl.pop().value() == 7);
  std::optional<int> none;
  CHECK(cl.popFor(none, 1ms) == PopStatus::Closed);
  auto token = std::make_shared<int>(0);
  Channel<std::shared_ptr<int>> ab(4);
  ab.push(std::shared_ptr<int>(token));
  ab.push(std::shared_ptr<int>(token));
  CHECK(token.use_count() == 3);
  ab.abort();
  CHECK(token.use_count() == 1);
  CHECK(ab.size() == 0);
  CHECK(!ab.tryPush(std::shared_ptr<int>(token)));
}

void testChannelThreads() {
  // One producer, one consumer, tiny queue: every item arrives once, in order.
  Channel<int> c(3);
  constexpr int kN = 50000;
  std::thread prod([&] {
    for (int i = 0; i < kN; ++i) c.push(int(i));
    c.close();
  });
  int expect = 0;
  bool ok = true;
  while (auto v = c.pop()) ok = ok && *v == expect++;
  prod.join();
  CHECK(ok);
  CHECK(expect == kN);
}

void testEmitterReuse() {
  Emitter<int> em;
  em.emit(1);
  em.emit(2);
  CHECK(em.size() == 2);
  CHECK(em.items()[1] == 2);
  const std::size_t cap = em.items().capacity();
  em.clear();
  CHECK(em.empty() && em.items().capacity() == cap);
  em.emit(3);
  std::vector<int> taken = em.take();  // the old API still works
  CHECK(taken.size() == 1 && taken[0] == 3 && em.empty());
}

// The steady-state path of single-worker stages (source, ordered and unordered
// blocks, sink) must not allocate: Emitter buffers are reused, single-worker
// stages skip the reorder map, and channels use a circular buffer.
void testNoSteadyStateAllocations() {
  constexpr int kWarmup = 2000, kMeasure = 20000;
  std::atomic<std::uint64_t> atStart{0}, atEnd{0};
  Pipeline p("no-alloc");
  int n = 0;
  auto src = p.sourceFn<int>("src", [&n](Emitter<int>& out) {
    out.emit(n++);
    return n < kWarmup + kMeasure;
  });
  auto a = p.map("ordered", src, [](int&& x) { return x + 1; }, opts().queue(4));
  auto b = p.blockFn<int>("unordered", a, [](int&& x, Emitter<int>& out) {
    out.emit(x);
    if (x % 2) out.emit(-x);  // variable fan-out reuses the same buffer
  }, opts().queue(4).unordered());
  int seen = 0;
  p.sinkFn("sink", b, [&](int&& x) {
    if (x < 0) return;
    ++seen;
    if (seen == kWarmup) atStart = allocationCount();
    if (seen == kWarmup + kMeasure) atEnd = allocationCount();
  }, opts().queue(4));
  p.start();
  p.wait();
  CHECK(p.state() == PipelineState::Finished);
  CHECK(seen == kWarmup + kMeasure);
  const std::uint64_t allocs = atEnd.load() - atStart.load();
  if (allocs != 0) std::cerr << "  " << allocs << " allocations in " << kMeasure << " items\n";
  CHECK(allocs == 0);
}

// A stage with parallel(1) but the default ordered mode, and a stage that can
// autoscale (so it keeps the reorder buffer), both keep their order.
void testSingleWorkerOrderedBypass() {
  std::vector<int> got;
  Pipeline p("bypass");
  auto src = p.source<Counter>("src", 3000);
  auto one = p.map("one", src, [](int&& x) { return x; });
  auto many = p.add<SlowDouble>("many", one, opts().autoscale(1, 3).queue(8));
  p.sink<Collect>("sink", many, opts(), &got);
  p.start();
  CHECK(p.waitFor(30s));
  CHECK(got.size() == 3000);
  bool inOrder = true;
  for (std::size_t i = 0; i < got.size(); ++i) inOrder = inOrder && got[i] == static_cast<int>(2 * i);
  CHECK(inOrder);
}

}  // namespace

int main() {
  const std::vector<std::pair<const char*, std::function<void()>>> tests = {
      {"ordered parallel", testOrderedParallel},
      {"unordered parallel", testUnorderedParallel},
      {"rate change + flush", testRateChangeAndFlush},
      {"controls", testControls},
      {"control value types", testControlValueTypes},
      {"controls while running", testControlsWhileRunning},
      {"lambda stages", testLambdas},
      {"errors: skip", testErrorsSkip},
      {"errors: stop", testErrorsStop},
      {"abort / stop / dtor", testAbortAndStop},
      {"autoscale", testAutoscale},
      {"sum monitors survive retirement", testSumSurvivesRetirement},
      {"feeder: end of stream", testFeederEndOfStream},
      {"feeder: stop drains, tryPush drops", testFeederStopDrainsAndTryPushDrops},
      {"pool", testPool},
      {"pool destructor", testPoolDestructorAborts},
      {"build errors", testBuildErrors},
      {"json", testJson},
      {"onUpdate", testOnUpdate},
      {"channel ring buffer", testChannelRing},
      {"channel threads", testChannelThreads},
      {"emitter reuse", testEmitterReuse},
      {"no steady-state allocations", testNoSteadyStateAllocations},
      {"single-worker ordered bypass", testSingleWorkerOrderedBypass},
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
