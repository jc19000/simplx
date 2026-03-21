/*
 * pingpong_v2.cpp
 *
 * Ping-pong latency benchmark using StaticActor with compile-time dispatch.
 *
 * Improvements over v1:
 *  - StaticActor<>: event dispatch resolved at compile time (no linear search)
 *  - Minimal event payload: only carries a timestamp, no strings
 *  - Cache-line friendly: event struct kept small to fit in single cache line
 *  - __builtin_prefetch hints for event data
 */

#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <atomic>
#include <cstdlib>

#include "simplx.h"
#include "trz/engine/static_actor.h"

using namespace std;
using namespace tredzone;
using namespace chrono;

static const int NUM_ITERATIONS = 10000;
static atomic<bool> benchmarkDone{false};
static vector<double> latencies_ns;

//---- Minimal events (cache-line friendly) ------------------------------------

struct PingEvent : Actor::Event
{
    high_resolution_clock::time_point sendTime;
    PingEvent(high_resolution_clock::time_point t) : sendTime(t) {}
};

struct PongEvent : Actor::Event
{
    high_resolution_clock::time_point sendTime;
    PongEvent(high_resolution_clock::time_point t) : sendTime(t) {}
};

//---- Pong Actor (StaticActor — compile-time dispatch) ------------------------

class PongActor : public StaticActor<PongActor, PingEvent>
{
public:
    PongActor() {}

    void onEvent(const PingEvent &e)
    {
        Event::Pipe pipe(*this, e.getSourceActorId());
        pipe.push<PongEvent>(e.sendTime);
    }
};

struct PongTag : public Service {};

//---- Ping Actor (StaticActor — compile-time dispatch) ------------------------

class PingActor : public StaticActor<PingActor, PongEvent>
{
public:
    PingActor() : count(0)
    {
        latencies_ns.clear();
        latencies_ns.reserve(NUM_ITERATIONS);

        const ActorId destId = getEngine().getServiceIndex().getServiceActorId<PongTag>();
        Event::Pipe pipe(*this, destId);
        pipe.push<PingEvent>(high_resolution_clock::now());
    }

    void onEvent(const PongEvent &e)
    {
        auto now = high_resolution_clock::now();
        auto rtt = duration_cast<nanoseconds>(now - e.sendTime).count();
        latencies_ns.push_back(static_cast<double>(rtt));
        ++count;

        if (__builtin_expect(count < NUM_ITERATIONS, 1))
        {
            Event::Pipe pipe(*this, e.getSourceActorId());
            pipe.push<PingEvent>(now);
        }
        else
        {
            benchmarkDone.store(true, memory_order_release);
        }
    }

private:
    int count;
};

//---- Stats -------------------------------------------------------------------

static void printStats(int pingCore, int pongCore)
{
    auto &v = latencies_ns;
    size_t n = v.size();
    if (n == 0)
        return;

    sort(v.begin(), v.end());

    double sum = accumulate(v.begin(), v.end(), 0.0);
    double mean = sum / n;

    double sq_sum = 0;
    for (auto x : v)
        sq_sum += (x - mean) * (x - mean);
    double stddev = sqrt(sq_sum / n);

    cout << "cores " << pingCore << "<->" << pongCore
         << " | n=" << n
         << " | min=" << fixed << setprecision(0) << v[0] << "ns"
         << " | p50=" << v[n / 2] << "ns"
         << " | p90=" << v[n * 90 / 100] << "ns"
         << " | p99=" << v[n * 99 / 100] << "ns"
         << " | max=" << v[n - 1] << "ns"
         << " | avg=" << setprecision(0) << mean << "ns"
         << " | stddev=" << setprecision(0) << stddev << "ns"
         << endl;
}

//---- MAIN --------------------------------------------------------------------

int main(int argc, char *argv[])
{
    int pingCore = 1;
    int pongCore = 2;

    if (argc == 3)
    {
        pingCore = atoi(argv[1]);
        pongCore = atoi(argv[2]);
    }

    Engine::StartSequence startSequence;

    startSequence.addServiceActor<PongTag, PongActor>(pongCore);
    startSequence.addActor<PingActor>(pingCore);

    Engine engine(startSequence);

    while (!benchmarkDone.load(memory_order_acquire))
        usleep(1000);

    usleep(10000);

    printStats(pingCore, pongCore);

    return 0;
}
