/**
 * @file microbench/main.cpp
 * @brief Microbenchmark — isolate and measure each component of the event dispatch hot path.
 *
 * Measures (in nanoseconds):
 *   1. rdtsc overhead                    — cost of the timing itself
 *   2. clock_gettime overhead             — std::chrono cost
 *   3. direct function call               — compiler baseline
 *   4. virtual dispatch                   — indirect call overhead
 *   5. CAS (compare_exchange)             — atomic operation cost
 *   6. cache line transfer (cross-core)   — L2-to-L2 latency
 *   7. Event::Pipe construction           — lookup destination pipe
 *   8. Event::Pipe::push<E>()             — allocate + construct + enqueue
 *   9. flushPipe()                        — force WriteCache to shared memory
 *  10. same-core round-trip / 2           — ping-pong on same AsyncNode
 *  11. cross-core round-trip              — ping-pong between 2 cores
 *
 * Usage: bench_microbench.bin [iterations]
 */

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <vector>
#include <cmath>
#include <chrono>
#include <thread>
#include <atomic>
#include <unistd.h>

#include "simplx.h"
#include "trz/engine/static_actor.h"
#include "../common/adaptive_event_loop.h"

using namespace std;
using namespace tredzone;
using namespace std::chrono;

// ---- Generic benchmark events -----------------------------------------------

using Timestamp = std::chrono::high_resolution_clock::time_point;

struct PingEvent : Actor::Event
{
    uint64_t seqNum;
    uint64_t payload;
    Timestamp ts;
    PingEvent(uint64_t seq, uint64_t p, Timestamp t) : seqNum(seq), payload(p), ts(t) {}
};

struct PongEvent : Actor::Event
{
    uint64_t seqNum;
    Timestamp ts;
    PongEvent(uint64_t seq, Timestamp t) : seqNum(seq), ts(t) {}
};

// ---- Timing -----------------------------------------------------------------

#if defined(__x86_64__) || defined(_M_X64)
static inline uint64_t rdtsc()
{
    unsigned lo, hi;
    __asm__ volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
}
static inline void fence() { __asm__ volatile("lfence" ::: "memory"); }
#elif defined(__aarch64__)
static inline uint64_t rdtsc()
{
    uint64_t val;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(val));
    return val;
}
static inline void fence() { __asm__ volatile("isb" ::: "memory"); }
#endif

static double g_ticksPerNs = 0;

static void calibrateTSC()
{
    auto t0 = high_resolution_clock::now();
    fence(); uint64_t c0 = rdtsc(); fence();
    usleep(100000);
    fence(); uint64_t c1 = rdtsc(); fence();
    auto t1 = high_resolution_clock::now();
    double ns = (double)duration_cast<nanoseconds>(t1 - t0).count();
    g_ticksPerNs = (double)(c1 - c0) / ns;
    printf("TSC: %.3f GHz (%.2f ticks/ns)\n", g_ticksPerNs, g_ticksPerNs);
}

static inline double toNs(uint64_t ticks) { return (double)ticks / g_ticksPerNs; }

// ---- Stats ------------------------------------------------------------------

struct BenchResult
{
    const char *name;
    vector<double> samples;

    void compute() { sort(samples.begin(), samples.end()); }
    double min_()  const { return samples.front(); }
    double p50()   const { return samples[samples.size() / 2]; }
    double p90()   const { return samples[samples.size() * 90 / 100]; }
    double p99()   const { return samples[samples.size() * 99 / 100]; }
    double p999()  const { return samples[samples.size() * 999 / 1000]; }
    double max_()  const { return samples.back(); }
    double avg()   const { return accumulate(samples.begin(), samples.end(), 0.0) / samples.size(); }
};

static void printResult(const BenchResult &r)
{
    printf("  %-40s  min=%6.1f  p50=%6.1f  p90=%6.1f  p99=%6.1f  p999=%7.1f  max=%8.1f  avg=%6.1f ns\n",
           r.name, r.min_(), r.p50(), r.p90(), r.p99(), r.p999(), r.max_(), r.avg());
}

static void printTSV(const BenchResult &r)
{
    printf("%s\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\n",
           r.name, r.min_(), r.p50(), r.p90(), r.p99(), r.p999(), r.max_(), r.avg());
}

// ---- CPU Primitive Benchmarks -----------------------------------------------

static volatile uint64_t g_sink = 0;
struct Payload { uint64_t a, b, c, d; };
static void __attribute__((noinline)) directFn(const Payload &p) { g_sink = p.a + p.b + p.c + p.d; }
struct VBase { virtual void fn(const Payload &) = 0; virtual ~VBase() = default; };
struct VImpl : VBase { void fn(const Payload &p) override { g_sink = p.a + p.b; } };

static BenchResult benchRdtsc(int N)
{
    BenchResult r{"rdtsc overhead", {}};
    r.samples.resize(N);
    for (int i = 0; i < 1000; ++i) { fence(); volatile auto t = rdtsc(); fence(); (void)t; }
    for (int i = 0; i < N; ++i) {
        fence(); uint64_t t0 = rdtsc(); fence();
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    r.compute(); return r;
}

static BenchResult benchClock(int N)
{
    BenchResult r{"clock_gettime (chrono)", {}};
    r.samples.resize(N);
    for (int i = 0; i < 1000; ++i) { volatile auto t = high_resolution_clock::now(); (void)t; }
    for (int i = 0; i < N; ++i) {
        fence(); uint64_t t0 = rdtsc(); fence();
        volatile auto t = high_resolution_clock::now(); (void)t;
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    r.compute(); return r;
}

static BenchResult benchDirectCall(int N)
{
    BenchResult r{"direct function call (noinline)", {}};
    r.samples.resize(N);
    Payload p{1,2,3,4};
    for (int i = 0; i < 1000; ++i) directFn(p);
    for (int i = 0; i < N; ++i) {
        p.a = i;
        fence(); uint64_t t0 = rdtsc(); fence();
        directFn(p);
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    r.compute(); return r;
}

static BenchResult benchVirtualCall(int N)
{
    BenchResult r{"virtual dispatch (indirect call)", {}};
    r.samples.resize(N);
    VImpl impl; VBase *base = &impl;
    Payload p{1,2,3,4};
    for (int i = 0; i < 1000; ++i) base->fn(p);
    for (int i = 0; i < N; ++i) {
        p.a = i;
        fence(); uint64_t t0 = rdtsc(); fence();
        base->fn(p);
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    r.compute(); return r;
}

static atomic<uint64_t> g_casTarget{0};

static BenchResult benchCAS(int N)
{
    BenchResult r{"CAS (compare_exchange acq_rel)", {}};
    r.samples.resize(N);
    g_casTarget.store(0);
    for (int i = 0; i < 1000; ++i) {
        uint64_t e = g_casTarget.load();
        g_casTarget.compare_exchange_strong(e, e+1, memory_order_acq_rel, memory_order_relaxed);
    }
    for (int i = 0; i < N; ++i) {
        uint64_t e = g_casTarget.load(memory_order_relaxed);
        fence(); uint64_t t0 = rdtsc(); fence();
        g_casTarget.compare_exchange_strong(e, e+1, memory_order_acq_rel, memory_order_relaxed);
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    r.compute(); return r;
}

static atomic<int> g_pp{0};
static volatile uint64_t g_sharedLine __attribute__((aligned(64))) = 0;

static BenchResult benchCacheLineTransfer(int N)
{
    BenchResult r{"cache line transfer (cross-core)", {}};
    r.samples.resize(N);
    atomic<bool> ready{false}, done{false};
    g_pp.store(1, memory_order_relaxed);  // writer must wait for reader's first g_pp=0

    thread writer([&]() {
#ifdef __linux__
        cpu_set_t cs; CPU_ZERO(&cs); CPU_SET(1, &cs);
        pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
#endif
        ready.store(true);
        for (int i = 0; i < N && !done.load(memory_order_relaxed); ++i) {
            while (g_pp.load(memory_order_acquire) != 0 && !done.load(memory_order_relaxed)) ;
            g_sharedLine = (uint64_t)i + 1;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            g_pp.store(1, memory_order_release);
        }
    });

    while (!ready.load()) ;
    for (int i = 0; i < N; ++i) {
        g_pp.store(0, memory_order_release);
        fence(); uint64_t t0 = rdtsc(); fence();
        while (g_pp.load(memory_order_acquire) != 1) ;
        volatile uint64_t val = g_sharedLine; (void)val;
        fence(); uint64_t t1 = rdtsc(); fence();
        r.samples[i] = toNs(t1 - t0);
    }
    done.store(true);
    g_pp.store(0, memory_order_release);
    writer.join();
    r.compute(); return r;
}

// ---- Engine Component Benchmarks --------------------------------------------

static atomic<int> g_benchPhase{0};
static vector<double> g_pipeSamples, g_pushSamples, g_pushRtSamples, g_flushSamples;
static vector<double> g_sameCoreSamples, g_crossCoreSamples;

// ---- Same-core hop: ping-pong on same AsyncNode -----------------------------
// BenchActor sends one SameCorePing to SameCoreTarget (same core).
// SameCoreTarget echoes SameCorePong carrying the original rdtsc timestamp.
// BenchActor measures (recv_tsc - orig_tsc) / 2 = single same-core hop.
// Each round-trip crosses 2 synchronize() boundaries.

struct SameCorePing : Actor::Event
{
    uint64_t sendTsc;
    SameCorePing(uint64_t tsc) : sendTsc(tsc) {}
};

struct SameCorePong : Actor::Event
{
    uint64_t origTsc;
    SameCorePong(uint64_t tsc) : origTsc(tsc) {}
};

class SameCoreTarget : public StaticActor<SameCoreTarget, SameCorePing>
{
public:
    SameCoreTarget() { g_id = getActorId(); }
    void onEvent(const SameCorePing &e)
    {
        Event::Pipe pipe(*this, e.getSourceActorId());
        pipe.push<SameCorePong>(e.sendTsc);
    }
    static Actor::ActorId g_id;
};
Actor::ActorId SameCoreTarget::g_id;

// ---- Sink actors for push benchmarks (never spin-wait on these) -------------

struct SinkTag : Service {};
class SinkActor : public StaticActor<SinkActor, PingEvent>
{
public:
    SinkActor() { g_id = getActorId(); }
    void onEvent(const PingEvent &) {}
    static Actor::ActorId g_id;
};
Actor::ActorId SinkActor::g_id;

struct RtSinkTag : Service {};
class RuntimeSinkActor : public Actor
{
public:
    RuntimeSinkActor()
    {
        registerEventHandler<PingEvent>(*this);
        g_id = getActorId();
    }
    void onEvent(const PingEvent &) {}
    static Actor::ActorId g_id;
};
Actor::ActorId RuntimeSinkActor::g_id;

// ---- Pong actor on core 1 for cross-core round-trip -------------------------

class PongActor : public StaticActor<PongActor, PingEvent>
{
public:
    PongActor() { g_id = getActorId(); }
    void onEvent(const PingEvent &e)
    {
        Event::Pipe pipe(*this, e.getSourceActorId());
        pipe.push<PongEvent>(e.seqNum, e.ts);
    }
    static Actor::ActorId g_id;
};
Actor::ActorId PongActor::g_id;

// ---- BenchActor: drives all engine benchmarks from core 0 -------------------

class BenchActor : public StaticActor<BenchActor, PongEvent, SameCorePong>, public Actor::Callback
{
public:
    BenchActor(int n) : N(n) { registerCallback(*this); }

    void onCallback() noexcept
    {
        switch (phase)
        {
        case 0:
            runLocalBenchmarks();
            phase = 1;
            // Start same-core ping-pong
            g_sameCoreSamples.resize(N);
            scIdx = 0;
            sendSameCorePing();
            break;
        case 2:
            // Start cross-core ping-pong
            startCrossCore();
            break;
        case 99:
            break;
        }
    }

    // Same-core pong received — record and send next ping (or move to cross-core)
    void onEvent(const SameCorePong &e)
    {
        fence(); uint64_t t1 = rdtsc(); fence();
        if (scIdx < N)
            g_sameCoreSamples[scIdx] = toNs(t1 - e.origTsc) / 2.0;
        ++scIdx;
        if (scIdx < N)
            sendSameCorePing();
        else {
            phase = 2;
            registerCallback(*this);  // trigger cross-core phase
        }
    }

    // Cross-core pong received
    void onEvent(const PongEvent &)
    {
        fence(); uint64_t t1 = rdtsc(); fence();
        if (ccIdx < N)
            g_crossCoreSamples[ccIdx] = toNs(t1 - ccStart);
        ++ccIdx;
        if (ccIdx < N)
            sendCrossCorePing();
        else {
            phase = 99;
            g_benchPhase.store(99, memory_order_release);
        }
    }

private:
    int N, phase = 0, scIdx = 0, ccIdx = 0;
    uint64_t ccStart = 0;

    void runLocalBenchmarks()
    {
        g_pipeSamples.resize(N);
        g_pushSamples.resize(N);
        g_pushRtSamples.resize(N);
        g_flushSamples.resize(N);

        // Warmup
        for (int i = 0; i < 100; ++i) {
            Event::Pipe p(*this, SinkActor::g_id);
            p.push<PingEvent>(0, 0, high_resolution_clock::now());
        }

        // Pipe construction
        for (int i = 0; i < N; ++i) {
            fence(); uint64_t t0 = rdtsc(); fence();
            Event::Pipe pipe(*this, SinkActor::g_id);
            fence(); uint64_t t1 = rdtsc(); fence();
            g_pipeSamples[i] = toNs(t1 - t0);
            pipe.push<PingEvent>(i, 0, high_resolution_clock::now());
        }

        // push<Event>() to StaticActor sink
        for (int i = 0; i < N; ++i) {
            Event::Pipe pipe(*this, SinkActor::g_id);
            fence(); uint64_t t0 = rdtsc(); fence();
            pipe.push<PingEvent>(i, 0, high_resolution_clock::now());
            fence(); uint64_t t1 = rdtsc(); fence();
            g_pushSamples[i] = toNs(t1 - t0);
        }

        // push<Event>() to Runtime dispatch actor
        for (int i = 0; i < N; ++i) {
            Event::Pipe pipe(*this, RuntimeSinkActor::g_id);
            fence(); uint64_t t0 = rdtsc(); fence();
            pipe.push<PingEvent>(i, 0, high_resolution_clock::now());
            fence(); uint64_t t1 = rdtsc(); fence();
            g_pushRtSamples[i] = toNs(t1 - t0);
        }

        // flushPipe()
        for (int i = 0; i < N; ++i) {
            Event::Pipe pipe(*this, SinkActor::g_id);
            pipe.push<PingEvent>(i, 0, high_resolution_clock::now());
            fence(); uint64_t t0 = rdtsc(); fence();
            flushPipe();
            fence(); uint64_t t1 = rdtsc(); fence();
            g_flushSamples[i] = toNs(t1 - t0);
        }
    }

    void sendSameCorePing()
    {
        Event::Pipe pipe(*this, SameCoreTarget::g_id);
        fence(); uint64_t tsc = rdtsc(); fence();
        pipe.push<SameCorePing>(tsc);
    }

    void sendCrossCorePing()
    {
        Event::Pipe pipe(*this, PongActor::g_id);
        fence(); ccStart = rdtsc(); fence();
        pipe.push<PingEvent>(ccIdx, 0, high_resolution_clock::now());
    }

    void startCrossCore()
    {
        g_crossCoreSamples.resize(N);
        ccIdx = 0;
        sendCrossCorePing();
    }
};

// ---- MAIN -------------------------------------------------------------------

int main(int argc, char *argv[])
{
    int N = (argc > 1) ? atoi(argv[1]) : 100000;
    if (N < 1000) N = 1000;

    printf("================================================================\n");
    printf("  SIMPLX MICROBENCHMARK - Hot Path Component Costs\n");
    printf("  %d iterations per test\n", N);
    printf("================================================================\n\n");

    // Phase 1: CPU primitives
    calibrateTSC();
    printf("\n--- CPU / Memory Primitives ---\n\n");

    vector<BenchResult> results;
    results.push_back(benchRdtsc(N));              printResult(results.back());
    results.push_back(benchClock(N));              printResult(results.back());
    results.push_back(benchDirectCall(N));         printResult(results.back());
    results.push_back(benchVirtualCall(N));        printResult(results.back());
    results.push_back(benchCAS(N));                printResult(results.back());
    results.push_back(benchCacheLineTransfer(N));  printResult(results.back());

    // Phase 2: Engine components
    printf("\n--- Simplx Engine Components ---\n\n");

    {
        Engine::StartSequence ss;
        static AdaptiveEventLoopFactory factory(EventLoopMode::LowLatency);
        ss.setEngineCustomEventLoopFactory(factory);

        // Core 0: BenchActor + SinkActor + RuntimeSinkActor + SameCoreTarget
        ss.addServiceActor<SinkTag, SinkActor>(0);
        ss.addServiceActor<RtSinkTag, RuntimeSinkActor>(0);
        ss.addActor<SameCoreTarget>(0);
        ss.addActor<BenchActor>(0, N);

        // Core 1: PongActor
        ss.addActor<PongActor>(1);

        Engine engine(ss);

        // Wait for all phases to complete
        int timeout = 0;
        while (g_benchPhase.load(memory_order_acquire) < 99 && timeout < 600) {
            usleep(100000);
            ++timeout;
        }
        usleep(200000);
    }

    // Print engine results
    auto makeResult = [](const char *name, vector<double> &v) -> BenchResult {
        BenchResult r{name, std::move(v)};
        r.compute();
        return r;
    };

    if (!g_pipeSamples.empty()) {
        results.push_back(makeResult("Pipe construction", g_pipeSamples));
        printResult(results.back());
    }
    if (!g_pushSamples.empty()) {
        results.push_back(makeResult("Pipe::push<E>() [StaticActor]", g_pushSamples));
        printResult(results.back());
    }
    if (!g_pushRtSamples.empty()) {
        results.push_back(makeResult("Pipe::push<E>() [runtime dispatch]", g_pushRtSamples));
        printResult(results.back());
    }
    if (!g_flushSamples.empty()) {
        results.push_back(makeResult("flushPipe()", g_flushSamples));
        printResult(results.back());
    }
    if (!g_sameCoreSamples.empty()) {
        results.push_back(makeResult("same-core round-trip/2 (single hop)", g_sameCoreSamples));
        printResult(results.back());
    }
    if (!g_crossCoreSamples.empty()) {
        results.push_back(makeResult("cross-core round-trip (2 hops)", g_crossCoreSamples));
        printResult(results.back());
    }

    // TSV
    printf("\n--- TSV ---\n");
    printf("component\tmin\tp50\tp90\tp99\tp999\tmax\tavg\n");
    for (auto &r : results) printTSV(r);

    // Derived
    printf("\n--- Derived Metrics (p50) ---\n\n");
    double crossP50 = 0, sameP50 = 0, pushP50 = 0, pipeP50 = 0, flushP50 = 0, clP50 = 0;
    for (auto &r : results) {
        string n = r.name;
        if (n.find("cross-core") != string::npos)  crossP50 = r.p50();
        if (n.find("same-core") != string::npos)   sameP50 = r.p50();
        if (n.find("StaticActor") != string::npos)  pushP50 = r.p50();
        if (n.find("Pipe constr") != string::npos)  pipeP50 = r.p50();
        if (n.find("flush") != string::npos)        flushP50 = r.p50();
        if (n.find("cache line") != string::npos)   clP50 = r.p50();
    }

    printf("  %-45s %7.1f ns\n", "Pipe construction:", pipeP50);
    printf("  %-45s %7.1f ns\n", "push<Event>():", pushP50);
    printf("  %-45s %7.1f ns\n", "flushPipe():", flushP50);
    printf("  %-45s %7.1f ns\n", "Framework overhead (pipe + push):", pipeP50 + pushP50);
    printf("  %-45s %7.1f ns\n", "Same-core single hop:", sameP50);
    printf("  %-45s %7.1f ns\n", "Same-core engine overhead (hop - push):", sameP50 - pushP50);
    printf("  %-45s %7.1f ns\n", "Cross-core round-trip:", crossP50);
    printf("  %-45s %7.1f ns\n", "Cross-core single hop:", crossP50 / 2.0);
    printf("  %-45s %7.1f ns\n", "Physical cache line transfer:", clP50);
    printf("  %-45s %7.1f ns\n", "Engine overhead per cross-core hop:",
           crossP50 / 2.0 - clP50);
    printf("\n  -> Same-core hop vs push-only: +%.1f ns (sync cycle + dispatch cost)\n",
           sameP50 - pushP50);
    printf("  -> Cross-core vs same-core: +%.1f ns (cache line transfer + cross-node sync)\n",
           crossP50 / 2.0 - sameP50);
    printf("  -> Cross-core is bounded by physics: %.1f ns (cache line transfer)\n", clP50);

    printf("\nDone.\n");
    return 0;
}
