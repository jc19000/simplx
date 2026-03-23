/**
 * @file spin_event_loop.h
 * @brief Custom spin-only event loop with configurable spin count before yield.
 *
 * Unlike DefaultEventLoop which unconditionally spins, this loop adds a
 * configurable "busy spin count" between yield calls, allowing fine-tuning
 * of CPU usage vs latency tradeoff.
 */

#pragma once

#include "simplx.h"

namespace tredzone
{

/**
 * @brief Spin-only EventLoop with tunable spin count.
 *
 * Spins for `spinCount` iterations using a pause instruction before
 * calling sched_yield(). Set spinCount=0 for pure spin (no yield).
 */
class SpinEventLoop : public EngineEventLoop
{
public:
    static unsigned s_spinCountBeforeYield;

protected:
    void run() noexcept override
    {
        while (isRunning())
        {
            do
            {
                synchronize();
                if (s_spinCountBeforeYield == 0)
                    continue;
                if (isInterrupted())
                    break;
                for (unsigned i = 0; i < s_spinCountBeforeYield; ++i)
                {
#if defined(__x86_64__) || defined(__i386__)
                    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
                    __asm__ __volatile__("yield" ::: "memory");
#endif
                }
            } while (!isInterrupted());
        }
    }
};

/**
 * @brief Factory that produces SpinEventLoop instances.
 */
class SpinEventLoopFactory : public EngineCustomEventLoopFactory
{
public:
    EventLoopAutoPointer newEventLoop() override
    {
        return EventLoopAutoPointer::newEventLoop<SpinEventLoop>();
    }
};

} // namespace tredzone
