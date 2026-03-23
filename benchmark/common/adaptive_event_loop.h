/**
 * @file adaptive_event_loop.h
 * @brief Configurable event loop with two modes: low-latency spin and system-friendly adaptive.
 *
 * LOW_LATENCY mode:
 *   Pure spin loop with pause/yield hints between synchronize() calls.
 *   Burns 100% CPU per core but minimizes event delivery latency.
 *   Use for: HFT, real-time processing where latency matters more than CPU cost.
 *
 * ADAPTIVE mode:
 *   Spins while events are being processed. After N consecutive idle cycles
 *   (no events delivered, no callbacks fired), falls back to usleep(sleepUs).
 *   Resumes spinning immediately when activity resumes.
 *   Use for: production services, batch processing, anything that should be a good OS citizen.
 *
 * Usage:
 *   AdaptiveEventLoopFactory factory(EventLoopMode::Adaptive, 256, 1);
 *   // or: AdaptiveEventLoopFactory factory(EventLoopMode::LowLatency);
 *   startSequence.setEngineCustomEventLoopFactory(factory);
 */

#pragma once

#include "simplx.h"
#include <unistd.h>

namespace tredzone
{

enum class EventLoopMode
{
    LowLatency, // pure spin — minimum latency, 100% CPU
    Adaptive     // spin when busy, usleep when idle
};

/**
 * @brief EventLoop configuration passed from the factory to each loop instance.
 */
struct EventLoopConfig
{
    EventLoopMode mode;
    unsigned idleThreshold;
    unsigned sleepUs;

    EventLoopConfig(EventLoopMode m = EventLoopMode::Adaptive,
                    unsigned idle = 256, unsigned sleep = 1)
        : mode(m), idleThreshold(idle), sleepUs(sleep) {}
};

class AdaptiveEventLoop : public EngineEventLoop
{
public:
    explicit AdaptiveEventLoop(const EventLoopConfig &config)
        : config(config) {}

protected:
    void run() noexcept override
    {
        if (config.mode == EventLoopMode::LowLatency)
            runLowLatency();
        else
            runAdaptive();
    }

private:
    EventLoopConfig config;

    static inline void cpuPauseHint() noexcept
    {
#if defined(__x86_64__) || defined(__i386__)
        __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
        __asm__ __volatile__("yield" ::: "memory");
#endif
    }

    void runLowLatency() noexcept
    {
        while (isRunning())
        {
            do
            {
                synchronize();
                if (isInterrupted())
                    break;
                cpuPauseHint();
            } while (!isInterrupted());
        }
    }

    void runAdaptive() noexcept
    {
        unsigned idleCycles = 0;

        while (isRunning())
        {
            do
            {
                synchronize();

                if (isInterrupted())
                    break;

                // lastSynchronizeWasIdle() returns true when the cycle processed
                // no events and no callbacks — the node had nothing to do.
                if (!lastSynchronizeWasIdle())
                {
                    idleCycles = 0;
                }
                else
                {
                    ++idleCycles;
                    if (idleCycles >= config.idleThreshold)
                    {
                        // Idle for too long — back off to save CPU.
                        // usleep(1) yields ~1µs on Linux, freeing the core.
                        // Wake-up latency adds ~1-5µs to the next event delivery.
                        usleep(config.sleepUs);
                    }
                    else
                    {
                        // Still in spin window — pause hint to save power
                        // and free the execution port for the sibling hyperthread.
                        cpuPauseHint();
                    }
                }
            } while (!isInterrupted());

            idleCycles = 0;
        }
    }
};

/**
 * @brief Factory that produces AdaptiveEventLoop instances with a given configuration.
 *
 * Usage:
 *   AdaptiveEventLoopFactory factory(EventLoopMode::Adaptive);
 *   AdaptiveEventLoopFactory factory(EventLoopMode::LowLatency);
 *   AdaptiveEventLoopFactory factory(EventLoopMode::Adaptive, 512, 5); // custom threshold/sleep
 *   startSequence.setEngineCustomEventLoopFactory(factory);
 */
class AdaptiveEventLoopFactory : public EngineCustomEventLoopFactory
{
public:
    explicit AdaptiveEventLoopFactory(EventLoopMode mode = EventLoopMode::Adaptive,
                                      unsigned idleThreshold = 256,
                                      unsigned sleepUs = 1)
        : config{mode, idleThreshold, sleepUs} {}

    explicit AdaptiveEventLoopFactory(const EventLoopConfig &config)
        : config(config) {}

    EventLoopAutoPointer newEventLoop() override
    {
        return EventLoopAutoPointer::newEventLoop<AdaptiveEventLoop, EventLoopConfig>(config);
    }

private:
    EventLoopConfig config;
};

} // namespace tredzone
