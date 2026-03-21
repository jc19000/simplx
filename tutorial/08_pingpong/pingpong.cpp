/*
 * main.cpp
 *
 * Ping-pong latency benchmark.
 * Measures round-trip latency between two actors on configurable CPU cores.
 */

#include <iostream>
#include <iomanip>
#include <string>
#include <chrono>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <atomic>

#include "simplx.h"

using namespace std;
using namespace tredzone;
using namespace chrono;

static const int NUM_ITERATIONS = 10000;
static atomic<bool> benchmarkDone{false};
static vector<double> latencies_ns;

//---- Ping Event (carries timestamp) ------------------------------------------

struct PingEvent : Actor::Event
{
	PingEvent(high_resolution_clock::time_point t) : sendTime(t) {}
	high_resolution_clock::time_point sendTime;
};

struct PongEvent : Actor::Event
{
	PongEvent(high_resolution_clock::time_point t) : sendTime(t) {}
	high_resolution_clock::time_point sendTime;
};

//---- Pong Actor Service ------------------------------------------------------

class PongActor : public Actor
{
public:
    PongActor()
	{
        registerEventHandler<PingEvent>(*this);
    }

    void onEvent(const PingEvent& e)
	{
        Event::Pipe pipe(*this, e.getSourceActorId());
		pipe.push<PongEvent>(e.sendTime);
    }
};

struct PongTag : public Service{};

//---- Ping Actor --------------------------------------------------------------

class PingActor : public Actor
{
public:
    PingActor()
		: count(0)
	{
        registerEventHandler<PongEvent>(*this);

		latencies_ns.clear();
		latencies_ns.reserve(NUM_ITERATIONS);

		// send first ping
		const ActorId destId = getEngine().getServiceIndex().getServiceActorId<PongTag>();
        Event::Pipe pipe(*this, destId);
		pipe.push<PingEvent>(high_resolution_clock::now());
    }

    void onEvent(const PongEvent& e)
	{
		auto rtt = duration_cast<nanoseconds>(high_resolution_clock::now() - e.sendTime).count();
		latencies_ns.push_back(static_cast<double>(rtt));
		count++;

		if (count < NUM_ITERATIONS)
		{
			// send next ping
			Event::Pipe pipe(*this, e.getSourceActorId());
			pipe.push<PingEvent>(high_resolution_clock::now());
		}
		else
		{
			benchmarkDone.store(true);
		}
    }

private:
	int count;
};

//---- Stats -------------------------------------------------------------------

static void printStats(int pingCore, int pongCore)
{
	auto& v = latencies_ns;
	size_t n = v.size();
	if (n == 0) return;

	sort(v.begin(), v.end());

	double sum = accumulate(v.begin(), v.end(), 0.0);
	double mean = sum / n;

	double sq_sum = 0;
	for (auto x : v) sq_sum += (x - mean) * (x - mean);
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

int main(int argc, char* argv[])
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

	// wait for benchmark to complete
	while (!benchmarkDone.load())
		usleep(1000);

	// small grace period
	usleep(10000);

	printStats(pingCore, pongCore);

	return 0;
}
