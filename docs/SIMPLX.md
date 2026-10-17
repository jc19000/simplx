# Coming from simplx

`cannes` is an independent Rust implementation of the model of
[Tredzone simplx](https://github.com/Tredzone/simplx). No simplx code is used.

| simplx (C++) | cannes |
|---|---|
| `Engine::StartSequence`, `addActor<A>(core)` | `StartSequence::new()`, `add_actor(core, \|\| a)` |
| `addServiceActor<Tag, A>(core)` / `getServiceActorId<Tag>()` | `add_service::<Tag, _, _>(core, \|\| a)` / `ctx.service::<Tag>()` |
| `Engine` constructor / destructor | `Engine::start(sequence)` / `Engine::shutdown()` or `Drop` |
| subclass of `Actor::Event` | any `Send + 'static` type (any size, alignment ≤ 16) |
| `registerEventHandler<E>(*this)` / `onEvent(const E&)` | `ctx.register_handler::<E, Self>()` / `Handler<E>::on_event(&mut self, ctx, src, &E)` |
| `Event::Pipe(*this, dst).push<E>(args...)` | `ctx.send(dst, event)` |
| `registerUndeliveredEventHandler<E>` / `onUndeliveredEvent(const E&)` | `ctx.register_undelivered_handler::<E, Self>()` / `UndeliveredHandler<E>` |
| `throw ReturnToSenderException()` | `ctx.return_to_sender()` |
| `newUnreferencedActor<A>()` | `ctx.new_actor(a)` |
| `newReferencedActor<A>()` / `ActorReference<A>` | `ctx.new_referenced_actor(a)` / `ActorRef<A>` (`r.borrow_mut().method()`) |
| `referenceLocalActor<A>(id)` (`CircularReferenceException`) | `ctx.reference_local_actor::<A>(id)` (`Err(ReferenceError::Circular)`, …) |
| `newReferencedSingletonActor<A>()` | `ctx.singleton(\|\| a)` |
| `requestDestroy()` / `onDestroyRequest()` / `acceptDestroy()` | `ctx.request_destroy()` / `on_destroy_request(&mut self, ctx) -> bool` |
| `registerCallback(*this)` / `onCallback()` | `ctx.register_callback()` / `on_callback()` |
| `TimerActor` (service `service::Timer`) / `TimerProxy` / `onTimeout()` | `TimerService` (service `TimerServiceTag`) / `Timer` / `Timeout` event |
| `getCorePerformanceCounters()` | `ctx.counters()`, `Engine::counters()`, `Engine::shutdown()` |
| publication at the end of the loop iteration | same, plus `ctx.flush()` |

## Same semantics

- **References:** a referenced actor is not destroyed while referenced (its destroy request is
  deferred); an actor created by `new_referenced_actor` or `singleton` is destroyed once its last
  reference is dropped; circular references are refused.
- **Shutdown:** regular actors are asked to destroy themselves first, services once no regular
  actor is left; an actor can postpone by returning `false` from `on_destroy_request`.
- **Timers:** one service keeps one deadline per client core and checks the clock from a callback
  while one is pending; one hub per core holds all timers of that core and only asks the service
  for the next deadline.
- **Counters:** `loop_total`, `loop_usage`, `events`, `callbacks`, bytes written per core, plus
  `publications` (ring Release stores, i.e. the batching factor).

## Differences

- **Timeouts are events.** The owner of a `Timer` receives a `Timeout` event (`timer.is(&t)` tells
  timers apart and ignores a timeout sent before the timer was re-armed or unset), instead of a
  direct `onTimeout()` call. A direct call from the hub would let `on_timeout` re-arm a timer while
  the hub is running, i.e. alias it.
- **Inter-core transport.** A bounded SPSC byte ring per pair of cores instead of simplx's
  handshake over chained event pages; see [DESIGN.md](DESIGN.md). Consequences: a fixed ring
  capacity (64 KiB by default, with local overflow when full) and a maximum event size of half a
  ring.
- **Errors.** A panic in a handler aborts the process; simplx catches exceptions per handler.
- **Checked by the compiler:** `ActorRef` is not `Send`; one writer and one reader per ring;
  events are `Send + 'static`; re-entrant calls panic instead of aliasing.

## Not implemented

Handler priorities, `BufferedPipe` and atomic grouped pushes, `registerPerformanceNeutralCallback`,
red zone / real-time thread priority, custom core actor and event loop factories, engine-to-engine
(inter-process) connectors.
