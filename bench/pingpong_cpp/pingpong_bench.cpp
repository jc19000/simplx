/*
 * pingpong_bench.cpp
 *
 * Inter-core round-trip latency benchmark for simplx.
 * PingActor (core A) sends a PingEvent to PongActor (service, core B), which echoes it back.
 * Ping only sends the next event once the previous one came back: one event in flight at a time.
 * Each round trip is timed with rdtsc; the TSC is calibrated against steady_clock over the run.
 *
 * usage: pingpong_bench [iterations=1000000] [warmup=100000] [ping_core=1] [pong_core=2] [spin|yield]
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <x86intrin.h>

#include "simplx.h"

using namespace tredzone;

static uint64_t g_Iterations = 1000000;
static uint64_t g_Warmup = 100000;
static std::vector<uint64_t> g_Samples; // round-trip times in TSC ticks
static uint64_t g_TotalTicks = 0;       // wall time of the measured phase, in TSC ticks

struct PingEvent : Actor::Event
{
    PingEvent(uint64_t s, uint64_t t) : seq(s), t0(t) {}
    uint64_t seq;
    uint64_t t0;
};

class PongActor : public Actor
{
  public:
    PongActor() { registerEventHandler<PingEvent>(*this); }

    void onEvent(const PingEvent &e)
    {
        Event::Pipe pipe(*this, e.getSourceActorId());
        pipe.push<PingEvent>(e.seq, e.t0);
    }
};

struct PongTag : Service
{
};

class PingActor : public Actor
{
  public:
    PingActor() : m_Pipe(*this, getEngine().getServiceIndex().getServiceActorId<PongTag>()), m_Count(0), m_Start(0)
    {
        registerEventHandler<PingEvent>(*this);
        g_Samples.reserve(g_Iterations);
        m_Pipe.push<PingEvent>(0, __rdtsc());
    }

    void onEvent(const PingEvent &e)
    {
        const uint64_t now = __rdtsc();
        if (m_Count >= g_Warmup)
            g_Samples.push_back(now - e.t0);
        else if (m_Count + 1 == g_Warmup)
            m_Start = now;

        if (++m_Count == g_Warmup + g_Iterations)
        {
            g_TotalTicks = now - m_Start;
            requestDestroy();
            return;
        }
        m_Pipe.push<PingEvent>(m_Count, __rdtsc());
    }

    void onDestroyRequest() noexcept override
    {
        if (m_Count == g_Warmup + g_Iterations)
            acceptDestroy();
        else
            requestDestroy();
    }

  private:
    Event::Pipe m_Pipe;
    uint64_t m_Count;
    uint64_t m_Start;
};

// Core-actor that busy-spins instead of calling threadYield() on every idle loop iteration.
struct SpinCoreActorFactory : EngineCustomCoreActorFactory
{
    struct SpinCoreActor : Actor
    {
    };

    Actor::ActorReference<Actor> newCustomCoreActor(Engine &, Actor::CoreId, bool, Actor &parent) override
    {
        return parent.newReferencedActor<SpinCoreActor>();
    }
};

int main(int argc, char **argv)
{
    if (argc > 1)
        g_Iterations = strtoull(argv[1], nullptr, 10);
    if (argc > 2)
        g_Warmup = strtoull(argv[2], nullptr, 10);
    const int pingCore = argc > 3 ? atoi(argv[3]) : 1;
    const int pongCore = argc > 4 ? atoi(argv[4]) : 2;
    const bool spin = argc > 5 ? strcmp(argv[5], "yield") != 0 : true;

    SpinCoreActorFactory spinFactory;
    Engine::StartSequence startSequence;
    if (spin)
        startSequence.setEngineCustomCoreActorFactory(spinFactory);
    startSequence.addServiceActor<PongTag, PongActor>(pongCore);
    startSequence.addActor<PingActor>(pingCore);

    const auto wall0 = std::chrono::steady_clock::now();
    const uint64_t tsc0 = __rdtsc();
    {
        Engine engine(startSequence); // returns once PingActor accepted destruction
    }
    const uint64_t tsc1 = __rdtsc();
    const auto wall1 = std::chrono::steady_clock::now();
    const double nsPerTick =
        std::chrono::duration<double, std::nano>(wall1 - wall0).count() / static_cast<double>(tsc1 - tsc0);

    std::sort(g_Samples.begin(), g_Samples.end());
    auto pct = [&](double p) {
        size_t i = static_cast<size_t>(p / 100.0 * (g_Samples.size() - 1));
        return g_Samples[i] * nsPerTick;
    };
    double sum = 0;
    for (uint64_t s : g_Samples)
        sum += s;
    const double meanNs = sum / g_Samples.size() * nsPerTick;
    const double totalNs = g_TotalTicks * nsPerTick;

    printf("impl=simplx-%s iterations=%llu cores=%d,%d\n", spin ? "spin" : "yield",
           (unsigned long long)g_Samples.size(), pingCore, pongCore);
    printf("rtt_ns min=%.0f p50=%.0f p90=%.0f p99=%.0f p99.9=%.0f p99.99=%.0f max=%.0f mean=%.1f\n", pct(0), pct(50),
           pct(90), pct(99), pct(99.9), pct(99.99), pct(100), meanNs);
    printf("throughput round_trips_per_s=%.0f\n", g_Samples.size() / (totalNs * 1e-9));
    return 0;
}
