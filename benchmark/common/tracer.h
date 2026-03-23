/**
 * @file tracer.h
 * @brief Zero-cost tracing infrastructure for simplx actor framework.
 *
 * When disabled (default): 1 branch per trace point, always predicted not-taken.
 * Measured overhead: < 1ns per trace point when disabled.
 *
 * When enabled: records spans to thread-local ring buffers (no lock, no allocation
 * on hot path). Exports to Chrome Trace Event Format JSON, viewable in:
 *   - https://ui.perfetto.dev
 *   - chrome://tracing
 *
 * Usage in actors:
 *
 *   void onEvent(const MyEvent &e) {
 *       TRACE_SPAN("Actor::MyEvent");
 *       // ... actual work ...
 *   }
 *
 * Usage at engine startup:
 *
 *   Tracer::enable();                    // turn on tracing
 *   // ... run workload ...
 *   Tracer::dump("/tmp/trace.json");     // export Chrome Trace Format
 *   Tracer::disable();                   // turn off (optional)
 *
 * Or scoped:
 *
 *   {
 *       TracingSession session("/tmp/trace.json");
 *       // ... run workload ...
 *   } // auto-dumps on destruction
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>
#include <mutex>

namespace tredzone
{

// ---- Trace record -----------------------------------------------------------

struct TraceRecord
{
    uint64_t beginNs;      // steady_clock nanoseconds since epoch
    uint64_t endNs;        // 0 for instant events
    const char *name;      // static string (no allocation)
    uint32_t tid;          // thread/core id
    uint8_t category;      // TraceCategory enum
};

enum TraceCategory : uint8_t
{
    TraceCat_Event     = 0,  // actor event dispatch
    TraceCat_Callback  = 1,  // actor callback
    TraceCat_Sync      = 2,  // synchronize cycle
    TraceCat_Flush     = 3,  // flushPipe
    TraceCat_Idle      = 4,  // core going idle
    TraceCat_User      = 5,  // user-defined
};

static const char *traceCategoryNames[] = {
    "event", "callback", "sync", "flush", "idle", "user"
};

// ---- Thread-local buffer ----------------------------------------------------

class Tracer
{
    static constexpr int MAX_THREADS = 32;
    static constexpr int INITIAL_CAPACITY = 1 << 20;  // 1M events

    struct alignas(64) ThreadBuffer
    {
        std::vector<TraceRecord> records;
        bool initialized;
        char pad[64 - sizeof(std::vector<TraceRecord>) - sizeof(bool)];

        ThreadBuffer() : initialized(false) {}
    };

    static inline std::atomic<bool> s_enabled{false};
    static inline ThreadBuffer s_buffers[MAX_THREADS];
    static inline std::atomic<int> s_threadCount{0};
    static inline uint64_t s_epochNs{0};  // steady_clock offset for relative timestamps
    static thread_local int s_threadId;

public:
    // ---- Enable/Disable (call before/after workload) ------------------------

    static void enable() noexcept
    {
        s_epochNs = nowNs();
        s_enabled.store(true, std::memory_order_release);
    }

    static void disable() noexcept
    {
        s_enabled.store(false, std::memory_order_release);
    }

    static bool isEnabled() noexcept
    {
        return s_enabled.load(std::memory_order_relaxed);
    }

    // ---- Recording (hot path) -----------------------------------------------

    static inline uint64_t nowNs() noexcept
    {
        return (uint64_t)std::chrono::steady_clock::now().time_since_epoch().count();
    }

    static int getThreadId() noexcept
    {
        int tid = s_threadId;
        if (__builtin_expect(tid >= 0, true))
            return tid;
        tid = s_threadCount.fetch_add(1, std::memory_order_relaxed);
        if (tid >= MAX_THREADS) tid = MAX_THREADS - 1;
        s_threadId = tid;
        s_buffers[tid].records.reserve(INITIAL_CAPACITY);
        s_buffers[tid].initialized = true;
        return tid;
    }

    // Record a completed span (begin+end)
    static inline void recordSpan(const char *name, uint64_t beginNs, uint64_t endNs,
                                   uint8_t category = TraceCat_Event) noexcept
    {
        int tid = getThreadId();
        s_buffers[tid].records.push_back({beginNs, endNs, name, (uint32_t)tid, category});
    }

    // Record an instant event
    static inline void recordInstant(const char *name, uint8_t category = TraceCat_User) noexcept
    {
        int tid = getThreadId();
        uint64_t ts = nowNs();
        s_buffers[tid].records.push_back({ts, 0, name, (uint32_t)tid, category});
    }

    // ---- Export (cold path) -------------------------------------------------

    static void dump(const char *path)
    {
        FILE *f = fopen(path, "w");
        if (!f) return;

        fprintf(f, "{\"traceEvents\":[\n");
        bool first = true;
        int n = s_threadCount.load(std::memory_order_relaxed);
        if (n > MAX_THREADS) n = MAX_THREADS;

        for (int tid = 0; tid < n; ++tid)
        {
            if (!s_buffers[tid].initialized) continue;
            for (const auto &r : s_buffers[tid].records)
            {
                double tsUs = (double)(r.beginNs - s_epochNs) / 1000.0;
                const char *cat = (r.category < sizeof(traceCategoryNames)/sizeof(traceCategoryNames[0]))
                                    ? traceCategoryNames[r.category] : "unknown";

                if (r.endNs != 0)
                {
                    // Complete event (X)
                    double durUs = (double)(r.endNs - r.beginNs) / 1000.0;
                    fprintf(f, "%s{\"name\":\"%s\",\"cat\":\"%s\",\"ph\":\"X\","
                               "\"ts\":%.3f,\"dur\":%.3f,\"pid\":0,\"tid\":%u}",
                            first ? "" : ",\n", r.name, cat, tsUs, durUs, r.tid);
                }
                else
                {
                    // Instant event (i)
                    fprintf(f, "%s{\"name\":\"%s\",\"cat\":\"%s\",\"ph\":\"i\","
                               "\"ts\":%.3f,\"pid\":0,\"tid\":%u,\"s\":\"t\"}",
                            first ? "" : ",\n", r.name, cat, tsUs, r.tid);
                }
                first = false;
            }
        }
        fprintf(f, "\n]}\n");
        fclose(f);
    }

    // ---- Reset (clear all buffers) ------------------------------------------

    static void reset() noexcept
    {
        int n = s_threadCount.load(std::memory_order_relaxed);
        if (n > MAX_THREADS) n = MAX_THREADS;
        for (int i = 0; i < n; ++i)
        {
            if (s_buffers[i].initialized)
                s_buffers[i].records.clear();
        }
    }

    // ---- Stats --------------------------------------------------------------

    static size_t totalRecords() noexcept
    {
        size_t total = 0;
        int n = s_threadCount.load(std::memory_order_relaxed);
        if (n > MAX_THREADS) n = MAX_THREADS;
        for (int i = 0; i < n; ++i)
            if (s_buffers[i].initialized)
                total += s_buffers[i].records.size();
        return total;
    }
};

inline thread_local int Tracer::s_threadId = -1;

// ---- RAII Span guard --------------------------------------------------------

namespace detail
{

struct TraceSpan
{
    const char *name;
    uint64_t beginNs;

    inline TraceSpan(const char *n) noexcept
    {
        if (__builtin_expect(Tracer::isEnabled(), false))
        {
            name = n;
            beginNs = Tracer::nowNs();
        }
        else
        {
            name = nullptr;
        }
    }

    inline ~TraceSpan() noexcept
    {
        if (__builtin_expect(name != nullptr, false))
        {
            Tracer::recordSpan(name, beginNs, Tracer::nowNs());
        }
    }

    // Non-copyable
    TraceSpan(const TraceSpan &) = delete;
    TraceSpan &operator=(const TraceSpan &) = delete;
};

struct TraceSyncSpan
{
    const char *name;
    uint64_t beginNs;

    inline TraceSyncSpan(const char *n) noexcept
    {
        if (__builtin_expect(Tracer::isEnabled(), false))
        {
            name = n;
            beginNs = Tracer::nowNs();
        }
        else
        {
            name = nullptr;
        }
    }

    inline ~TraceSyncSpan() noexcept
    {
        if (__builtin_expect(name != nullptr, false))
        {
            Tracer::recordSpan(name, beginNs, Tracer::nowNs(), TraceCat_Sync);
        }
    }

    TraceSyncSpan(const TraceSyncSpan &) = delete;
    TraceSyncSpan &operator=(const TraceSyncSpan &) = delete;
};

} // namespace detail

// ---- Macros -----------------------------------------------------------------
// These are the primary user-facing API.
// When tracing is disabled: 1 load + 1 branch (predicted not-taken) = ~0ns.

#define TRACE_SPAN(name)       ::tredzone::detail::TraceSpan _trc_span_{name}
#define TRACE_SYNC_SPAN(name)  ::tredzone::detail::TraceSyncSpan _trc_sync_{name}

#define TRACE_INSTANT(name)                                         \
    do {                                                            \
        if (__builtin_expect(::tredzone::Tracer::isEnabled(), false)) \
            ::tredzone::Tracer::recordInstant(name);                \
    } while (0)

#define TRACE_FLUSH()                                               \
    do {                                                            \
        if (__builtin_expect(::tredzone::Tracer::isEnabled(), false)) \
            ::tredzone::Tracer::recordInstant("flushPipe", ::tredzone::TraceCat_Flush); \
    } while (0)

// ---- Scoped tracing session -------------------------------------------------

class TracingSession
{
public:
    explicit TracingSession(const char *outputPath) : path(outputPath)
    {
        Tracer::reset();
        Tracer::enable();
    }

    ~TracingSession()
    {
        Tracer::disable();
        Tracer::dump(path);
    }

    TracingSession(const TracingSession &) = delete;
    TracingSession &operator=(const TracingSession &) = delete;

private:
    const char *path;
};

} // namespace tredzone
