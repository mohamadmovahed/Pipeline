// Quickstart: the smallest useful pipeline.
//   source -> scale (class with a control + monitor) -> heavy (4 workers) -> sink
#include <pipeline/pipeline.hpp>

#include <chrono>
#include <cmath>
#include <iostream>
#include <thread>
#include <vector>

using namespace pipeline;

// A block with a UI control ("gain") and a UI monitor ("peak").
class Scale : public Block<std::vector<float>, std::vector<float>> {
 public:
  void process(std::vector<float>&& in, Emitter<std::vector<float>>& out) override {
    float peak = 0;
    for (float& x : in) {
      x *= static_cast<float>(gain_.get());
      peak = std::max(peak, std::abs(x));
    }
    peak_ = peak;
    out.emit(std::move(in));
  }

 private:
  Control<double>& gain_ = control<double>("gain", 1.0, "linear gain", {0.0, 100.0});
  Monitor<double>& peak_ = monitor<double>("peak", "peak magnitude of the last block", Aggregate::Max);
};

int main() {
  Pipeline p("quickstart");

  int n = 0;
  auto src = p.sourceFn<std::vector<float>>("source", [n](Emitter<std::vector<float>>& out) mutable {
    std::vector<float> v(1024);
    for (std::size_t i = 0; i < v.size(); ++i) v[i] = std::sin(0.01f * static_cast<float>(i + 1024 * n));
    out.emit(std::move(v));
    return ++n < 2000;  // 2000 blocks, then end of stream
  });

  auto scaled = p.add<Scale>("scale", src);

  // Wrap an existing library function; run it on 4 threads, order is kept.
  auto energy = p.map("energy", scaled, [](std::vector<float>&& v) {
    double e = 0;
    for (int rep = 0; rep < 50; ++rep)  // pretend this is expensive
      for (float x : v) e += static_cast<double>(x) * x;
    return e / 50;
  }, opts().parallel(4));

  double total = 0;
  p.sinkFn("sink", energy, [&total](double&& e) { total += e; });

  p.setControl("scale.gain", 2.0);
  p.start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  std::cout << p.snapshotJson() << "\n\n";
  p.wait();  // until the source ends and everything is drained
  std::cout << "total energy = " << total << ", peak = " << toString(p.getMonitor("scale.peak")) << "\n";
}
