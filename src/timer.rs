//! Timers, built like simplx's (`trz/util/timer`):
//!
//! - `TimerService` (simplx `TimerActor`) runs as a service on one core. It keeps one pending
//!   deadline per client core and, while any is pending, checks the clock from a callback on each
//!   loop iteration of its core. When a deadline passes it sends a `Tick` to that core's hub.
//! - `TimerHub` (simplx `TimerProxy::SingletonActorEventHandler`) is a referenced singleton on each
//!   core using timers. It holds every timer of the core and only asks the service for the next
//!   deadline, so one event crosses cores per tick whatever the number of timers.
//! - `Timer` (simplx `TimerProxy`) is the handle an actor owns. It references the hub, so the hub
//!   lives as long as a timer of its core does.
//!
//! One difference with simplx: instead of calling a virtual `onTimeout()` on the proxy, the hub
//! sends a `Timeout` event to the owning actor, which handles it like any other event (several
//! timers per actor are told apart with `Timer::is`). Calling the owner directly from the hub's
//! handler would let `on_timeout` re-arm a timer while the hub is running, i.e. alias it.

use std::fmt;
use std::time::{Duration, Instant};

use crate::engine::{Actor, ActorId, ActorRef, Context, Handler};

/// Service tag of the timer service (simplx `service::Timer`).
pub struct TimerServiceTag;

/// hub -> service: tick me at `deadline` (replaces the core's previous request).
struct Wake {
    deadline: Instant,
}

/// service -> hub: a requested deadline has passed.
struct Tick {
    now: Instant,
}

/// Sent to the actor owning the timer when it expires.
pub struct Timeout {
    slot: u32,
    seq: u32,
    /// Clock reading of the timer service when it noticed the deadline.
    pub now: Instant,
}

// ---- Service -----------------------------------------------------------------------------------

/// Add with `sequence.add_service::<TimerServiceTag, _, _>(core, TimerService::new)`.
#[derive(Default)]
pub struct TimerService {
    clients: Vec<Option<(ActorId, Instant)>>, // by client core: (hub, deadline)
    pending: usize,
    armed: bool,
}

impl TimerService {
    /// A service with no client yet.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Actor for TimerService {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Wake, Self>();
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        let now = Instant::now();
        for client in self.clients.iter_mut() {
            if let Some((hub, deadline)) = *client {
                if deadline <= now {
                    ctx.send(hub, Tick { now });
                    *client = None;
                    self.pending -= 1;
                }
            }
        }
        self.armed = self.pending > 0;
        if self.armed {
            ctx.register_callback();
        }
    }
}

impl Handler<Wake> for TimerService {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, wake: &Wake) {
        let core = src.core as usize;
        if self.clients.len() <= core {
            self.clients.resize(core + 1, None);
        }
        if self.clients[core].is_none() {
            self.pending += 1;
        }
        self.clients[core] = Some((src, wake.deadline));
        if !self.armed {
            self.armed = true;
            ctx.register_callback();
        }
    }
}

// ---- Per-core hub ------------------------------------------------------------------------------

struct Entry {
    owner: ActorId,
    seq: u32, // bumped on each set/unset, so a Timeout already sent for an older arming is ignored
    deadline: Option<Instant>,
    period: Option<Duration>,
}

/// Per-core singleton holding the timers of its core; created and referenced by `Timer::new`.
pub struct TimerHub {
    id: ActorId,
    service: ActorId,
    entries: Vec<Entry>,
    free: Vec<u32>,
    requested: Option<Instant>,
}

impl TimerHub {
    fn new(service: ActorId) -> Self {
        Self { id: ActorId::NULL, service, entries: Vec::new(), free: Vec::new(), requested: None }
    }

    fn create(&mut self) -> u32 {
        let entry = Entry { owner: ActorId::NULL, seq: 0, deadline: None, period: None };
        match self.free.pop() {
            Some(slot) => {
                let seq = self.entries[slot as usize].seq;
                self.entries[slot as usize] = Entry { seq, ..entry };
                slot
            }
            None => {
                self.entries.push(entry);
                self.entries.len() as u32 - 1
            }
        }
    }

    fn arm(&mut self, ctx: &mut Context, slot: u32, owner: ActorId, deadline: Instant, period: Option<Duration>) {
        let entry = &mut self.entries[slot as usize];
        entry.seq = entry.seq.wrapping_add(1);
        entry.owner = owner;
        entry.deadline = Some(deadline);
        entry.period = period;
        self.request(ctx, deadline);
    }

    fn disarm(&mut self, slot: u32) {
        let entry = &mut self.entries[slot as usize];
        entry.seq = entry.seq.wrapping_add(1);
        entry.deadline = None;
    }

    fn release(&mut self, slot: u32) {
        self.disarm(slot);
        self.free.push(slot);
    }

    /// Asks the service for `deadline` unless an earlier tick is already requested.
    fn request(&mut self, ctx: &mut Context, deadline: Instant) {
        if self.requested.is_none_or(|r| deadline < r) {
            self.requested = Some(deadline);
            // Possibly called through a Timer while its owner is running: send as the hub.
            ctx.send_from(self.id, self.service, Wake { deadline });
        }
    }
}

impl Actor for TimerHub {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Tick, Self>();
    }
}

impl Handler<Tick> for TimerHub {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, tick: &Tick) {
        let now = tick.now;
        self.requested = None;
        let mut next: Option<Instant> = None;
        for (slot, entry) in self.entries.iter_mut().enumerate() {
            let Some(deadline) = entry.deadline else { continue };
            if deadline <= now {
                ctx.send(entry.owner, Timeout { slot: slot as u32, seq: entry.seq, now });
                // Like simplx, a repeating timer restarts from the tick, it does not catch up.
                entry.deadline = entry.period.map(|p| now + p);
            }
            if let Some(d) = entry.deadline {
                next = Some(next.map_or(d, |n| n.min(d)));
            }
        }
        if let Some(deadline) = next {
            self.request(ctx, deadline);
        }
    }
}

// ---- Timer handle ------------------------------------------------------------------------------

/// `Timer::new` was called but no `TimerService` was added to the start sequence.
#[derive(Debug, PartialEq, Eq)]
pub struct NoTimerService;

impl fmt::Display for NoTimerService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no TimerService registered in the start sequence")
    }
}

impl std::error::Error for NoTimerService {}

/// A timer owned by an actor (simplx `TimerProxy`). Create it from the actor's `on_start`; the
/// actor receives a `Timeout` event when it expires, and must handle it (`Handler<Timeout>`).
/// Dropping the timer cancels it.
pub struct Timer {
    hub: ActorRef<TimerHub>,
    slot: u32,
}

impl Timer {
    /// A new, unarmed timer owned by the current actor.
    pub fn new(ctx: &mut Context) -> Result<Timer, NoTimerService> {
        let service = ctx.service::<TimerServiceTag>().ok_or(NoTimerService)?;
        let hub = ctx.singleton(|| TimerHub::new(service));
        let slot = {
            let mut h = hub.borrow_mut();
            h.id = hub.id();
            h.create()
        };
        Ok(Timer { hub, slot })
    }

    /// Fires once, `after` from now (`Duration::ZERO`: on the next tick, simplx `setNow`).
    pub fn set(&self, ctx: &mut Context, after: Duration) {
        let owner = ctx.id();
        self.hub.borrow_mut().arm(ctx, self.slot, owner, Instant::now() + after, None);
    }

    /// Fires every `period`, each period starting when the previous timeout was noticed.
    pub fn set_repeat(&self, ctx: &mut Context, period: Duration) {
        let owner = ctx.id();
        self.hub.borrow_mut().arm(ctx, self.slot, owner, Instant::now() + period, Some(period));
    }

    /// Cancels the timer; a `Timeout` already sent is ignored by `is`.
    pub fn unset(&self) {
        self.hub.borrow_mut().disarm(self.slot);
    }

    /// Whether the timer is armed.
    pub fn is_set(&self) -> bool {
        self.hub.borrow().entries[self.slot as usize].deadline.is_some()
    }

    /// Whether `timeout` comes from this timer's current arming (a timeout sent before the timer
    /// was re-armed or unset returns false).
    pub fn is(&self, timeout: &Timeout) -> bool {
        timeout.slot == self.slot && self.hub.borrow().entries[self.slot as usize].seq == timeout.seq
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.hub.borrow_mut().release(self.slot);
    }
}
