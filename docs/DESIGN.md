# Design notes and measurements

## Inter-core rings

`src/bytering.rs`: one bounded lock-free SPSC ring of variable-size records per ordered pair of
cores.

- `head` is written only by the consumer, `tail` only by the producer, each on its own pair of
  cache lines.
- Each side keeps a local copy of the other side's index and reloads it only when the ring looks
  full (producer) or empty (consumer).
- Records are contiguous and 16-byte aligned; when one does not fit before the end of the buffer,
  a padding record fills the gap and the record starts at offset 0.
- Writes are staged and published by a single Release store of `tail` (`commit`); reads are
  released by a single Release store of `head`.
- `Producer` and `Consumer` are themselves 128-byte aligned (see below why).

A record is a 48-byte header (length, kind, source, destination, `TypeId`, drop function) followed
by the event. The handler receives `&E` pointing into the ring; the event is dropped in place
afterwards, then its slot is released.

## Event loop

Per iteration, on each core:

1. Drain every inbound ring, dispatching each record to its handler (lookup by `TypeId` in the
   actor's small handler table), then release the ring once.
2. Dispatch same-core events (double-buffered, so handlers can keep sending locally).
3. Run registered callbacks, then destroy requests.
4. Publish: one `commit` per destination core that received events (or earlier with
   `ctx.flush()`).
5. Update the counters.

## Where the first prototype lost 300 ns: false sharing

The first version of the engine was about 300 ns per round trip slower than two bare threads
exchanging through the same rings, although the engine only adds a loop over the rings. The cause
was false sharing. The local state of `Producer` 1→2 and `Producer` 2→1, written on every send,
sat on the same cache line: both had been allocated side by side by the main thread, and moving a
`Vec` to another thread keeps its buffer. Every send by one core invalidated the other core's line,
in both directions. The bare-thread version did not suffer from it because each thread owned its
own endpoint. Fix: `Producer` and `Consumer` are 128-byte aligned. Result: from ~890 ns to
~600 ns p50, 10 to 20 ns above the bare threads.

## One write per loop iteration is visible

When references were added, the loop emptied the list of unreferenced actors on every iteration
(borrow of a `RefCell`, `mem::take` of a `Vec`). That moved the minimum round trip from ~190 ns to
~370 ns and the p50 by ~40 ns, although these are a few core-local writes with no cache line
shared (checked by printing addresses: each core has its own allocator arena). It is now a read of
a `Cell<bool>` that is only written when a reference is dropped. On a hot path of a few dozen
instructions, every write per iteration shows.

Adding the performance counters (plain core-local `u64`) and the manual flush, on the other hand,
had no measurable cost: an A/B of the two builds, interleaved, gave p50 512 to 535 ns before and
517 to 533 ns after.

## Why simplx is slower

This is an analysis of simplx's code (`include/trz/engine/internal/parallel.h`, `node.h`), not a
fine-grained measurement.

simplx does not use a ring between two cores but a one-slot handshake:

1. The writing core accumulates its events in chained pages (`writeCache`).
2. At the end of its loop it can only hand them over if the previous batch was acknowledged
   (`isWriteLocked == false`). It then swaps its chains with the shared `ReadWriteLocked`
   structure (several fields over more than one cache line; the code itself has a
   `TODO make it all fit in one cache line`), recycles returned pages, issues an `mfence` and sets
   `isWriteLocked = true`.
3. The reading core sees the flag, walks the event chain (pointers into pages written by the other
   core), dispatches, issues an `mfence` and sets `isWriteLocked = false` to acknowledge.

Per hop, several cache lines travel between cores (flag, shared structure, event pages,
acknowledgement, the writer resetting the structure) plus two `mfence`. The ring moves two: the
`tail` index and the record's line. At 60 to 100 ns per cache-line transfer on the test machine,
that accounts for the order of magnitude of the ~390 ns extra per hop. The handshake has real
advantages (no fixed capacity, detection of a stopped core, no maximum event size), but it is
structurally more expensive in latency. Confirming it precisely would take `perf c2c` on bare
metal.

## Benchmark method

`examples/pingpong.rs` and `bench/simplx-cpp/pingpong_bench.cpp` do the same thing: Ping on one
core sends a 16-byte event to Pong (a service) on another core, which sends it back; the next
event is only sent once the previous one came back. Each round trip is timed with `rdtsc`, and the
TSC is calibrated against the monotonic clock over the run. 100 k warm-up round trips, then 1 M
measured. In both languages the engine is shut down only after the measurement (destroying the
simplx engine early would keep it in destroy-requested mode during the run). simplx is measured in
two modes: its default core actor, which calls `sched_yield()` on idle iterations, and a busy-spin
core actor, which is the fair comparison.

Throughput is computed over the whole measured phase, so it includes the time Ping spends between
receiving and resending.

On a shared VM, single runs sometimes show outliers (p50 around 100 to 200 ns), most likely two
vCPUs on the same physical core; tails beyond p99 mostly reflect the hypervisor. Compare medians of
interleaved runs, and prefer a tuned bare-metal machine (`isolcpus`, `nohz_full`, fixed frequency,
cores on the same socket).
