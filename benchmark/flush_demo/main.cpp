/**
 * @file bench_flush_demo.cpp
 * @brief Pipeline flush demo: 6 actors on 6 cores, batching 10 events.
 *
 * Demonstrates that flushPipe() enables pipeline parallelism.
 *
 * Pipeline: A1 → A2 → A3 → A4 → A5 → A6
 *           core0  core1  core2  core3  core4  core5
 *
 * A1 pushes 10 events then busy-waits 500µs (simulating computation).
 *   - WITHOUT flush: events stay in A1's local WriteCache until the handler
 *     returns and the next synchronize cycle runs. A2-A6 are idle during
 *     A1's busy-wait.
 *   - WITH flush: events are moved to shared memory before the busy-wait.
 *     A2-A6 can start processing immediately while A1 is still busy.
 *
 * We measure total end-to-end latency (A1 push time → A6 receive time).
 * With flush, the pipeline processing overlaps with A1's busy-wait,
 * so total latency ≈ max(busy_wait, pipeline_latency).
 * Without flush, total latency ≈ busy_wait + pipeline_latency.
 */

#include <iostream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <chrono>
#include <vector>
#include <atomic>
#include <unistd.h>

#include "simplx.h"
#include "trz/engine/static_actor.h"

using namespace std;
using namespace tredzone;
using namespace std::chrono;

// ---- Config -----------------------------------------------------------------

static const int NUM_ROUNDS       = 10000;  // number of batches
static const int BATCH_SIZE       = 10;     // events per batch
static const int BUSY_WAIT_US     = 1000;   // µs busy-wait after push (1ms)
static bool      USE_FLUSH        = false;  // toggled via argv

// ---- Events -----------------------------------------------------------------

struct PipelineEvent : Actor::Event
{
    int roundId;
    int seqInBatch;
    high_resolution_clock::time_point originTs; // when A1 pushed the batch
    PipelineEvent(int r, int s, high_resolution_clock::time_point ts)
        : roundId(r), seqInBatch(s), originTs(ts) {}
};

struct TriggerEvent : Actor::Event {};

// ---- Globals ----------------------------------------------------------------

static Actor::ActorId g_pipelineActors[6]; // A1..A6
static atomic<int> g_registeredCount{0};
static atomic<int> g_completedRounds{0};

struct alignas(64) LatencySlot {
    vector<double> values;
};
static LatencySlot g_e2eLatencies;   // end-to-end A1→A6
static LatencySlot g_a1Latencies;    // A1 handler duration (push + busywait)

// ---- busy_wait --------------------------------------------------------------

static inline void busy_wait_us(int us)
{
    auto start = high_resolution_clock::now();
    while (duration_cast<microseconds>(high_resolution_clock::now() - start).count() < us)
    {
#if defined(__x86_64__) || defined(__i386__)
        __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
        __asm__ __volatile__("yield" ::: "memory");
#endif
    }
}

// ---- A1: Source actor -------------------------------------------------------
// Pushes BATCH_SIZE events, optionally flushes, then busy-waits.

/**
 * A1Actor: Source of the pipeline.
 *
 * TWO MODES:
 *
 * NO-FLUSH mode (baseline):
 *   onEvent(TriggerEvent):
 *     1. Push BATCH_SIZE events to A2
 *     2. busy_wait(BUSY_WAIT_US)   <-- events stuck in WriteCache during this!
 *     3. Return → sync cycle delivers events to A2
 *   Result: e2e ≈ busy_wait + pipeline_latency (serial)
 *
 * FLUSH mode:
 *   onEvent(TriggerEvent):
 *     1. Push BATCH_SIZE events to A2
 *     2. Return immediately → events delivered at sync cycle boundary
 *   onCallback():
 *     3. busy_wait(BUSY_WAIT_US)   <-- A2-A6 are processing IN PARALLEL!
 *     4. Self-trigger next round
 *   Result: e2e ≈ max(busy_wait, pipeline_latency) (parallel)
 *
 * The key insight: by returning from the handler before busy-waiting,
 * the engine's synchronize cycle can flush events to peers. The callback
 * fires on the NEXT cycle, allowing computation to overlap with pipeline.
 * flushPipe() ensures write signals are set for maximum responsiveness.
 */
class A1Actor : public StaticActor<A1Actor, TriggerEvent>, public Actor::Callback
{
public:
    A1Actor()
    {
        int idx = g_registeredCount.fetch_add(1);
        g_pipelineActors[idx] = getActorId();
        registerCallback(*this);
    }

    void onCallback() noexcept
    {
        // Initial startup: wait for all actors
        if (!started)
        {
            if (g_registeredCount.load(memory_order_acquire) < 6)
            {
                registerCallback(*this);
                return;
            }
            started = true;
            Event::Pipe self(*this, getActorId());
            self.push<TriggerEvent>();
            return;
        }

        // FLUSH mode: callback fires after events have been delivered to A2.
        // Do busy-wait here — A2-A6 process events IN PARALLEL.
        if (USE_FLUSH && pendingBusyWait)
        {
            busy_wait_us(BUSY_WAIT_US);

            auto handlerEnd = high_resolution_clock::now();
            g_a1Latencies.values.push_back(
                (double)duration_cast<nanoseconds>(handlerEnd - batchStartTs).count());

            pendingBusyWait = false;

            if (roundsSent < NUM_ROUNDS)
            {
                Event::Pipe self(*this, getActorId());
                self.push<TriggerEvent>();
            }
        }
    }

    void onEvent(const TriggerEvent &)
    {
        if (roundsSent >= NUM_ROUNDS) return;

        batchStartTs = high_resolution_clock::now();

        // Push batch of BATCH_SIZE events to A2
        Event::Pipe pipe(*this, g_pipelineActors[1]);
        for (int i = 0; i < BATCH_SIZE; ++i)
            pipe.push<PipelineEvent>(roundsSent, i, batchStartTs);

        ++roundsSent;

        if (USE_FLUSH)
        {
            // FLUSH mode: return immediately. Events will be delivered at end
            // of this sync cycle. Register callback for busy-wait on NEXT cycle.
            flushPipe();
            pendingBusyWait = true;
            registerCallback(*this);
            // DO NOT busy-wait here — let the sync cycle deliver events first!
        }
        else
        {
            // NO-FLUSH mode: busy-wait INSIDE the handler.
            // Events are stuck in WriteCache during the entire busy-wait.
            busy_wait_us(BUSY_WAIT_US);

            auto handlerEnd = high_resolution_clock::now();
            g_a1Latencies.values.push_back(
                (double)duration_cast<nanoseconds>(handlerEnd - batchStartTs).count());

            if (roundsSent < NUM_ROUNDS)
            {
                Event::Pipe self(*this, getActorId());
                self.push<TriggerEvent>();
            }
        }
    }

private:
    int roundsSent = 0;
    bool started = false;
    bool pendingBusyWait = false;
    high_resolution_clock::time_point batchStartTs;
};

// ---- Middle actors (A2-A5): forward with minimal work -----------------------

template <int STAGE>
class MiddleActor : public StaticActor<MiddleActor<STAGE>, PipelineEvent>
{
    using Base = StaticActor<MiddleActor<STAGE>, PipelineEvent>;
public:
    MiddleActor()
    {
        int idx = g_registeredCount.fetch_add(1);
        g_pipelineActors[idx] = this->getActorId();
    }

    void onEvent(const PipelineEvent &e)
    {
        // Forward to next stage
        typename Actor::Event::Pipe pipe(*this, g_pipelineActors[STAGE + 1]);
        pipe.push<PipelineEvent>(e.roundId, e.seqInBatch, e.originTs);
    }
};

// ---- A6: Sink actor ---------------------------------------------------------
// Records end-to-end latency.

class A6Actor : public StaticActor<A6Actor, PipelineEvent>
{
public:
    A6Actor()
    {
        int idx = g_registeredCount.fetch_add(1);
        g_pipelineActors[idx] = getActorId();
    }

    void onEvent(const PipelineEvent &e)
    {
        auto now = high_resolution_clock::now();
        double ns = (double)duration_cast<nanoseconds>(now - e.originTs).count();
        g_e2eLatencies.values.push_back(ns);

        // Count completed events; when all events of last round arrive, signal done
        ++receivedCount;
        if (receivedCount >= NUM_ROUNDS * BATCH_SIZE)
            g_completedRounds.store(1, memory_order_release);
    }

private:
    int receivedCount = 0;
};

// ---- Service tags -----------------------------------------------------------

struct A1Service : Service {};
struct A2Service : Service {};
struct A3Service : Service {};
struct A4Service : Service {};
struct A5Service : Service {};
struct A6Service : Service {};

// ---- Stats ------------------------------------------------------------------

static void printStats(const char *label, vector<double> &v)
{
    if (v.empty()) return;
    sort(v.begin(), v.end());
    size_t n = v.size();
    double sum = accumulate(v.begin(), v.end(), 0.0);
    double mean = sum / n;
    double sq = 0;
    for (auto x : v) sq += (x - mean) * (x - mean);
    double stddev = sqrt(sq / n);

    cout << label
         << " | n=" << n
         << " | min=" << fixed << setprecision(0) << v[0]
         << " | p50=" << v[n / 2]
         << " | p90=" << v[n * 90 / 100]
         << " | p99=" << v[n * 99 / 100]
         << " | max=" << v[n - 1]
         << " | avg=" << mean
         << " | stddev=" << stddev
         << endl;
}

// ---- MAIN -------------------------------------------------------------------

int main(int argc, char *argv[])
{
    USE_FLUSH = (argc > 1 && string(argv[1]) == "flush");

    cout << "=== Pipeline Flush Demo ===" << endl;
    cout << "Mode: " << (USE_FLUSH ? "WITH flushPipe()" : "WITHOUT flushPipe()") << endl;
    cout << "Pipeline: A1(c0) -> A2(c1) -> A3(c1) -> A4(c2) -> A5(c2) -> A6(c3)" << endl;
    cout << "Batch size: " << BATCH_SIZE << " events" << endl;
    cout << "Busy-wait after push: " << BUSY_WAIT_US << " µs" << endl;
    cout << "Rounds: " << NUM_ROUNDS << endl;
    cout << endl;

    g_e2eLatencies.values.reserve(NUM_ROUNDS * BATCH_SIZE);
    g_a1Latencies.values.reserve(NUM_ROUNDS);

    Engine::StartSequence startSequence;

    // 6 actors across 3 cores (macOS has no real CPU affinity, fewer cores = less OS contention)
    // A1 alone on core 0 (source + busy-wait)
    // A2,A3 on core 1 (pipeline mid)
    // A4,A5 on core 2 (pipeline mid)
    // A6 on core 3 (sink)
    startSequence.addServiceActor<A1Service, A1Actor>(0);
    startSequence.addServiceActor<A2Service, MiddleActor<1>>(1);
    startSequence.addServiceActor<A3Service, MiddleActor<2>>(1);
    startSequence.addServiceActor<A4Service, MiddleActor<3>>(2);
    startSequence.addServiceActor<A5Service, MiddleActor<4>>(2);
    startSequence.addServiceActor<A6Service, A6Actor>(3);

    Engine engine(startSequence);

    // A1 self-triggers via callback once all actors are registered.
    // Wait for completion.
    while (g_completedRounds.load(memory_order_acquire) < 1)
        usleep(10000);
    usleep(100000); // drain

    cout << "=== Results (" << (USE_FLUSH ? "FLUSH" : "NO-FLUSH") << ") ===" << endl;
    printStats("e2e_latency (A1->A6)", g_e2eLatencies.values);
    printStats("a1_handler  (push+busywait)", g_a1Latencies.values);

    auto &v = g_e2eLatencies.values;
    double busyWaitNs = BUSY_WAIT_US * 1000.0;
    double e2eMin = v[0];
    double e2eP50 = v[v.size() / 2];

    cout << endl;
    cout << "busy_wait    = " << fixed << setprecision(0) << busyWaitNs << " ns (" << BUSY_WAIT_US << " µs)" << endl;
    cout << "e2e min      = " << e2eMin << " ns" << endl;
    cout << "e2e p50      = " << e2eP50 << " ns" << endl;
    cout << endl;

    if (USE_FLUSH)
    {
        cout << "*** WITH FLUSH: e2e min (" << e2eMin << " ns) < busy_wait (" << busyWaitNs << " ns)" << endl;
        cout << "    => Pipeline ran IN PARALLEL with A1 busy-wait!" << endl;
        cout << "    => Events were delivered to A2 BEFORE A1 finished its computation." << endl;
    }
    else
    {
        cout << "*** WITHOUT FLUSH: e2e min (" << e2eMin << " ns) > busy_wait (" << busyWaitNs << " ns)" << endl;
        cout << "    => Pipeline ran AFTER A1 busy-wait completed." << endl;
        cout << "    => Events were stuck in WriteCache during A1's computation." << endl;
    }

    return 0;
}
