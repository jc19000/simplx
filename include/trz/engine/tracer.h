/**
 * @file tracer.h
 * @brief Distributed tracing infrastructure integrated into the tredzone engine.
 *
 * Automatically traces event dispatch with full causality tracking:
 *   - Each trace_id groups all events in a causal chain
 *   - span_id / parent_span_id form a tree showing which event triggered which
 *   - No instrumentation needed in user actor code
 *
 * When disabled (default): ~0ns overhead per trace point (1 load + 1 branch).
 * Event trace context propagation: 2 TLS reads + 2 stores per event creation (~2ns).
 *
 * When enabled: records spans to thread-local ring buffers (no lock, no allocation
 * on hot path). Exports to ClickHouse for Grafana visualization.
 *
 * Usage:
 *   tredzone::TracingSession session;
 *   session.enableClickHouse("localhost", 8123, "simplx", "traces");
 *   session.start();
 *   // ... run workload ...
 *   session.stop();
 *   // Traces in ClickHouse: SELECT * FROM simplx.traces WHERE trace_id = X ORDER BY offset_ns
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace tredzone
{

// ---- Trace categories -------------------------------------------------------

enum TraceCategory : uint8_t
{
    TraceCat_Event    = 0,  // actor event dispatch
    TraceCat_Callback = 1,  // actor callback invocation
    TraceCat_Sync     = 2,  // synchronize cycle
    TraceCat_User     = 3,  // user-defined spans
};

static const char *traceCategoryNames[] = {
    "event", "callback", "sync", "user"
};

// ---- Trace record -----------------------------------------------------------

struct TraceRecord
{
    uint64_t beginNs;       // steady_clock nanoseconds since epoch
    uint64_t endNs;
    uint64_t traceId;       // groups all spans in a causal chain
    uint64_t spanId;        // this span's unique ID
    uint64_t parentSpanId;  // parent span (0 for root)
    uint32_t tid;           // thread/core id
    uint8_t  category;      // TraceCategory enum
    uint16_t eventClassId;  // event type ID (0 if not an event span)
};

// ---- Tracer singleton -------------------------------------------------------

class Tracer
{
    static constexpr int MAX_THREADS = 64;
    static constexpr int INITIAL_CAPACITY = 1 << 20;  // 1M events

    struct alignas(64) ThreadBuffer
    {
        std::vector<TraceRecord> records;
        bool initialized;
        char pad[64 - sizeof(std::vector<TraceRecord>) - sizeof(bool)];

        ThreadBuffer() : initialized(false) {}
    };

    static std::atomic<bool> s_enabled;
    static ThreadBuffer s_buffers[MAX_THREADS];
    static std::atomic<int> s_threadCount;
    static uint64_t s_epochNs;

    // Thread-local state — defined in engine.cpp to avoid duplicate symbols
    static thread_local int s_threadId;
    static thread_local uint64_t s_currentTraceId;
    static thread_local uint64_t s_currentSpanId;
    static thread_local uint64_t s_spanCounter;

public:
    // ---- Enable/Disable -----------------------------------------------------

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

    // ---- Trace context (read by EventBase constructor) ----------------------

    static inline uint64_t currentTraceId() noexcept { return s_currentTraceId; }
    static inline uint64_t currentSpanId() noexcept { return s_currentSpanId; }

    static inline uint64_t nextSpanId() noexcept
    {
        // Top 16 bits = thread ID, bottom 48 bits = counter
        return ((uint64_t)(getThreadId() & 0xFFFF) << 48) | (++s_spanCounter);
    }

    static inline uint64_t nextTraceId() noexcept
    {
        // Use steady_clock nanoseconds — unique enough for single-process traces
        return nowNs();
    }

    // Set the current trace context (called by TraceEventGuard)
    static inline void setContext(uint64_t traceId, uint64_t spanId) noexcept
    {
        s_currentTraceId = traceId;
        s_currentSpanId = spanId;
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

    // Record a span with full trace context
    static inline void recordSpan(uint64_t traceId, uint64_t spanId, uint64_t parentSpanId,
                                   uint64_t beginNs, uint64_t endNs,
                                   uint8_t category, uint16_t eventClassId = 0) noexcept
    {
        int tid = getThreadId();
        s_buffers[tid].records.push_back({beginNs, endNs, traceId, spanId, parentSpanId,
                                           (uint32_t)tid, category, eventClassId});
    }

    // ---- Export: ClickHouse -------------------------------------------------

    static void dumpClickHouse(const char * /*host*/ = "localhost", int /*port*/ = 8123,
                                const char *database = "simplx", const char *table = "traces",
                                const char *sessionId = nullptr)
    {
        char autoSessionId[64];
        if (!sessionId)
        {
            auto now = std::chrono::system_clock::now();
            auto epoch = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            snprintf(autoSessionId, sizeof(autoSessionId), "trace-%lld", (long long)epoch);
            sessionId = autoSessionId;
        }

        char cmd[1024];
        snprintf(cmd, sizeof(cmd),
            "docker exec -i clickhouse clickhouse-client --query "
            "'INSERT INTO %s.%s FORMAT TabSeparated'",
            database, table);

        FILE *p = popen(cmd, "w");
        if (!p) return;

        int n = s_threadCount.load(std::memory_order_relaxed);
        if (n > MAX_THREADS) n = MAX_THREADS;

        for (int tid = 0; tid < n; ++tid)
        {
            if (!s_buffers[tid].initialized) continue;
            for (const auto &r : s_buffers[tid].records)
            {
                uint64_t offsetNs = r.beginNs - s_epochNs;
                uint64_t durationNs = (r.endNs > r.beginNs) ? (r.endNs - r.beginNs) : 0;
                const char *cat = (r.category < sizeof(traceCategoryNames)/sizeof(traceCategoryNames[0]))
                                    ? traceCategoryNames[r.category] : "user";

                fprintf(p, "%s\t%llu\t%llu\t%llu\t%llu\t%llu\t%u\t%s\t%u\n",
                        sessionId,
                        (unsigned long long)r.traceId,
                        (unsigned long long)r.spanId,
                        (unsigned long long)r.parentSpanId,
                        (unsigned long long)offsetNs,
                        (unsigned long long)durationNs,
                        r.tid,
                        cat,
                        (unsigned)r.eventClassId);
            }
        }
        pclose(p);
    }

    // ---- Export: Chrome Trace Format JSON ------------------------------------

    static void dumpChromeTrace(const char *path)
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
                double durUs = (r.endNs > r.beginNs) ? (double)(r.endNs - r.beginNs) / 1000.0 : 0;
                const char *cat = (r.category < sizeof(traceCategoryNames)/sizeof(traceCategoryNames[0]))
                                    ? traceCategoryNames[r.category] : "user";

                char nameBuf[64];
                if (r.category == TraceCat_Event)
                    snprintf(nameBuf, sizeof(nameBuf), "event:%u", (unsigned)r.eventClassId);
                else
                    snprintf(nameBuf, sizeof(nameBuf), "%s", cat);

                // Include trace context as args for Perfetto
                fprintf(f, "%s{\"name\":\"%s\",\"cat\":\"%s\",\"ph\":\"X\","
                           "\"ts\":%.3f,\"dur\":%.3f,\"pid\":0,\"tid\":%u,"
                           "\"args\":{\"trace_id\":\"%llu\",\"span_id\":\"%llu\",\"parent_span_id\":\"%llu\"}}",
                        first ? "" : ",\n", nameBuf, cat, tsUs, durUs, r.tid,
                        (unsigned long long)r.traceId,
                        (unsigned long long)r.spanId,
                        (unsigned long long)r.parentSpanId);
                first = false;
            }
        }
        fprintf(f, "\n]}\n");
        fclose(f);
    }

    // ---- Reset / Stats ------------------------------------------------------

    static void reset() noexcept
    {
        int n = s_threadCount.load(std::memory_order_relaxed);
        if (n > MAX_THREADS) n = MAX_THREADS;
        for (int i = 0; i < n; ++i)
            if (s_buffers[i].initialized)
                s_buffers[i].records.clear();
    }

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

// ---- RAII guards (used by engine internals) ---------------------------------

namespace detail
{

// Placed at the top of EventTable::onEvent() — traces event dispatch with
// full causality: reads incoming event's trace context, creates a child span,
// sets thread-local context so downstream Pipe::push() events inherit it.
struct TraceEventGuard
{
    uint64_t beginNs;
    uint64_t traceId;
    uint64_t spanId;
    uint64_t parentSpanId;
    uint64_t prevTraceId;
    uint64_t prevSpanId;
    uint16_t classId;

    inline TraceEventGuard(uint16_t eventClassId, uint64_t eventTraceId, uint64_t eventTraceSpanId) noexcept
    {
        if (__builtin_expect(Tracer::isEnabled(), false))
        {
            beginNs = Tracer::nowNs();
            classId = eventClassId;

            // Save previous context (for nested dispatch)
            prevTraceId = Tracer::currentTraceId();
            prevSpanId = Tracer::currentSpanId();

            // Inherit trace ID from incoming event, or generate new one (root)
            traceId = (eventTraceId != 0) ? eventTraceId : Tracer::nextTraceId();
            parentSpanId = eventTraceSpanId; // 0 if root event
            spanId = Tracer::nextSpanId();

            // Set context so events sent during this handler inherit our span
            Tracer::setContext(traceId, spanId);
        }
        else
        {
            beginNs = 0;
        }
    }

    inline ~TraceEventGuard() noexcept
    {
        if (__builtin_expect(beginNs != 0, false))
        {
            Tracer::recordSpan(traceId, spanId, parentSpanId,
                                beginNs, Tracer::nowNs(),
                                TraceCat_Event, classId);
            // Restore previous context
            Tracer::setContext(prevTraceId, prevSpanId);
        }
    }

    TraceEventGuard(const TraceEventGuard &) = delete;
    TraceEventGuard &operator=(const TraceEventGuard &) = delete;
};

struct TraceCallbackGuard
{
    uint64_t beginNs;
    uint64_t traceId;
    uint64_t spanId;

    inline TraceCallbackGuard() noexcept
    {
        if (__builtin_expect(Tracer::isEnabled(), false))
        {
            beginNs = Tracer::nowNs();
            traceId = Tracer::currentTraceId();
            spanId = Tracer::nextSpanId();
        }
        else
        {
            beginNs = 0;
        }
    }

    inline ~TraceCallbackGuard() noexcept
    {
        if (__builtin_expect(beginNs != 0, false))
            Tracer::recordSpan(traceId, spanId, Tracer::currentSpanId(),
                                beginNs, Tracer::nowNs(), TraceCat_Callback);
    }

    TraceCallbackGuard(const TraceCallbackGuard &) = delete;
    TraceCallbackGuard &operator=(const TraceCallbackGuard &) = delete;
};

struct TraceSyncGuard
{
    uint64_t beginNs;

    inline TraceSyncGuard() noexcept
    {
        if (__builtin_expect(Tracer::isEnabled(), false))
            beginNs = Tracer::nowNs();
        else
            beginNs = 0;
    }

    inline ~TraceSyncGuard() noexcept
    {
        if (__builtin_expect(beginNs != 0, false))
            Tracer::recordSpan(Tracer::currentTraceId(), 0, 0,
                                beginNs, Tracer::nowNs(), TraceCat_Sync);
    }

    TraceSyncGuard(const TraceSyncGuard &) = delete;
    TraceSyncGuard &operator=(const TraceSyncGuard &) = delete;
};

} // namespace detail

// ---- Macros for engine internals --------------------------------------------

// TRZ_TRACE_EVENT reads trace context from the event for causality tracking
#define TRZ_TRACE_EVENT(eventClassId, eventTraceId, eventTraceSpanId) \
    ::tredzone::detail::TraceEventGuard _trz_trc_evt_{eventClassId, eventTraceId, eventTraceSpanId}

#define TRZ_TRACE_CALLBACK()  ::tredzone::detail::TraceCallbackGuard _trz_trc_cb_
#define TRZ_TRACE_SYNC()      ::tredzone::detail::TraceSyncGuard _trz_trc_sync_

// ---- Scoped tracing session -------------------------------------------------

class TracingSession
{
public:
    explicit TracingSession() : chromePath(nullptr), chHost(nullptr) {}

    void enableChromeTrace(const char *outputPath) { chromePath = outputPath; }

    void enableClickHouse(const char *host = "localhost", int port = 8123,
                           const char *database = "simplx", const char *table = "traces",
                           const char *sessionId = nullptr)
    {
        chHost = host;
        chPort = port;
        chDatabase = database;
        chTable = table;
        chSessionId = sessionId;
    }

    void start()
    {
        Tracer::reset();
        Tracer::enable();
    }

    void stop()
    {
        Tracer::disable();
        if (chromePath)
            Tracer::dumpChromeTrace(chromePath);
        if (chHost)
            Tracer::dumpClickHouse(chHost, chPort, chDatabase, chTable, chSessionId);
    }

    ~TracingSession()
    {
        if (Tracer::isEnabled())
            stop();
    }

    TracingSession(const TracingSession &) = delete;
    TracingSession &operator=(const TracingSession &) = delete;

private:
    const char *chromePath;
    const char *chHost;
    int chPort = 8123;
    const char *chDatabase = "simplx";
    const char *chTable = "traces";
    const char *chSessionId = nullptr;
};

} // namespace tredzone
