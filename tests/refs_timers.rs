//! Referenced actors, singletons and timers. Each test runs real pinned, spinning event loops, so
//! they are serialized.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use simplx_rs::{
    Actor, ActorId, ActorRef, Context, Engine, Handler, NoTimerService, ReferenceError, StartSequence, Timeout, Timer,
    TimerService, TimerServiceTag,
};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

type Log = Arc<Mutex<Vec<String>>>;

fn log(log: &Log, line: impl Into<String>) {
    log.lock().unwrap().push(line.into());
}

fn lines(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for(l: &Log, line: &str) {
    wait_until(line, || lines(l).iter().any(|x| x == line));
}

// ---- Referenced actors -------------------------------------------------------------------------

struct Counter {
    total: u64,
    log: Log,
}

struct Add(u64);

impl Counter {
    fn add(&mut self, n: u64) -> u64 {
        self.total += n;
        self.total
    }
}

impl Actor for Counter {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Add, Self>();
    }

    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        log(&self.log, format!("counter destroyed at {}", self.total));
        true
    }
}

impl Handler<Add> for Counter {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Add) {
        let total = self.add(e.0);
        log(&self.log, format!("event add -> {total}"));
    }
}

struct Parent {
    counter: Option<ActorRef<Counter>>,
    ticks: u32,
    log: Log,
}

impl Actor for Parent {
    fn on_start(&mut self, ctx: &mut Context) {
        let counter = ctx.new_referenced_actor(Counter { total: 0, log: self.log.clone() });
        // Direct, synchronous call: no event, no loop iteration.
        let total = counter.borrow_mut().add(5);
        log(&self.log, format!("direct add -> {total}"));
        ctx.send(counter.id(), Add(1));
        self.counter = Some(counter);
        ctx.register_callback();
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        self.ticks += 1;
        if self.ticks == 100 {
            log(&self.log, "parent drops reference");
            self.counter = None; // last reference: the counter is destroyed
        } else {
            ctx.register_callback();
        }
    }
}

#[test]
fn referenced_actor_direct_calls_and_destroyed_when_unreferenced() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    let l2 = l.clone();
    seq.add_actor(1, move || Parent { counter: None, ticks: 0, log: l2 });
    let engine = Engine::start(seq);
    wait_for(&l, "counter destroyed at 6");
    engine.shutdown();
    assert_eq!(lines(&l), vec!["direct add -> 5", "event add -> 6", "parent drops reference", "counter destroyed at 6"]);
}

/// Asks for its own destruction right away; must wait until its holder releases it.
struct EagerToDie {
    log: Log,
}

impl Actor for EagerToDie {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.request_destroy();
    }

    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        log(&self.log, "eager destroyed");
        true
    }
}

struct Holder {
    target: ActorId,
    held: Option<ActorRef<EagerToDie>>,
    ticks: u32,
    log: Log,
}

impl Actor for Holder {
    fn on_start(&mut self, ctx: &mut Context) {
        self.held = Some(ctx.reference_local_actor::<EagerToDie>(self.target).expect("reference"));
        ctx.register_callback();
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        self.ticks += 1;
        if self.ticks == 100 {
            log(&self.log, "holder releases");
            self.held = None;
        } else {
            ctx.register_callback();
        }
    }
}

#[test]
fn destroy_request_waits_until_unreferenced() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    let l2 = l.clone();
    let target = seq.add_actor(1, move || EagerToDie { log: l2 });
    let l2 = l.clone();
    seq.add_actor(1, move || Holder { target, held: None, ticks: 0, log: l2 });
    let engine = Engine::start(seq);
    wait_for(&l, "eager destroyed");
    engine.shutdown();
    assert_eq!(lines(&l), vec!["holder releases", "eager destroyed"]);
}

struct A {
    b: Option<ActorRef<B>>,
    remote: ActorId,
    log: Log,
}

struct B {
    a: ActorId,
    remote: ActorId,
    log: Log,
}

impl Actor for A {
    fn on_start(&mut self, ctx: &mut Context) {
        let me = ctx.id();
        self.b = Some(ctx.new_referenced_actor(B { a: me, remote: self.remote, log: self.log.clone() }));
    }
}

impl Actor for B {
    fn on_start(&mut self, ctx: &mut Context) {
        // A already references B: B referencing A would be circular.
        let r = ctx.reference_local_actor::<A>(self.a).err();
        log(&self.log, format!("B->A {r:?}"));
        let r = ctx.reference_local_actor::<Counter>(self.a).err();
        log(&self.log, format!("B->A as Counter {r:?}"));
        let r = ctx.reference_local_actor::<A>(self.remote).err();
        log(&self.log, format!("B->remote {r:?}"));
        let me = ctx.id();
        let r = ctx.reference_local_actor::<B>(me).err();
        log(&self.log, format!("B->B {r:?}"));
    }
}

struct Idle;
impl Actor for Idle {}

#[test]
fn reference_errors() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    let remote = seq.add_actor(2, || Idle);
    let l2 = l.clone();
    seq.add_actor(1, move || A { b: None, remote, log: l2 });
    let engine = Engine::start(seq);
    wait_until("4 lines", || lines(&l).len() == 4);
    engine.shutdown();
    assert_eq!(
        lines(&l),
        vec![
            format!("B->A {:?}", Some(ReferenceError::Circular)),
            format!("B->A as Counter {:?}", Some(ReferenceError::WrongType)),
            format!("B->remote {:?}", Some(ReferenceError::NotLocal)),
            format!("B->B {:?}", Some(ReferenceError::Circular)),
        ]
    );
}

// ---- Singletons --------------------------------------------------------------------------------

struct PerCore {
    users: u32,
    log: Log,
}

impl Actor for PerCore {}

impl Drop for PerCore {
    fn drop(&mut self) {
        log(&self.log, format!("singleton dropped after {} users", self.users));
    }
}

struct User {
    singleton: Option<ActorRef<PerCore>>,
    log: Log,
}

impl Actor for User {
    fn on_start(&mut self, ctx: &mut Context) {
        let l = self.log.clone();
        let s = ctx.singleton(move || PerCore { users: 0, log: l });
        let users = {
            let mut s = s.borrow_mut();
            s.users += 1;
            s.users
        };
        log(&self.log, format!("core {} user {users} singleton {:?}", ctx.core(), s.id()));
        self.singleton = Some(s);
        ctx.request_destroy();
    }
}

#[test]
fn singleton_is_shared_per_core_and_dropped_when_unreferenced() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    for core in [1, 1, 2] {
        let l2 = l.clone();
        seq.add_actor(core, move || User { singleton: None, log: l2 });
    }
    let engine = Engine::start(seq);
    wait_until("both singletons dropped", || lines(&l).iter().filter(|x| x.starts_with("singleton dropped")).count() == 2);
    engine.shutdown();
    let got = lines(&l);
    let ids = |core: u32| -> Vec<String> {
        got.iter()
            .filter(|x| x.starts_with(&format!("core {core} ")))
            .map(|x| x.split(" singleton ").nth(1).unwrap().to_string())
            .collect()
    };
    let (core1, core2) = (ids(1), ids(2));
    assert_eq!(core1.len(), 2);
    assert_eq!(core1[0], core1[1], "same instance on core 1");
    assert_eq!(core2.len(), 1);
    assert!(got.iter().any(|x| x.starts_with("core 1 user 2")));
    let mut dropped: Vec<_> = got.iter().filter(|x| x.starts_with("singleton dropped")).cloned().collect();
    dropped.sort();
    assert_eq!(dropped, vec!["singleton dropped after 1 users", "singleton dropped after 2 users"]);
}

// ---- Timers ------------------------------------------------------------------------------------

struct Clocked {
    once: Option<Timer>,
    repeat: Option<Timer>,
    cancelled: Option<Timer>,
    started: Instant,
    repeats: u32,
    log: Log,
}

impl Actor for Clocked {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Timeout, Self>();
        let once = Timer::new(ctx).unwrap();
        let repeat = Timer::new(ctx).unwrap();
        let cancelled = Timer::new(ctx).unwrap();
        self.started = Instant::now();
        once.set(ctx, Duration::from_millis(20));
        repeat.set_repeat(ctx, Duration::from_millis(5));
        cancelled.set(ctx, Duration::from_millis(10));
        cancelled.unset();
        assert!(once.is_set() && repeat.is_set() && !cancelled.is_set());
        (self.once, self.repeat, self.cancelled) = (Some(once), Some(repeat), Some(cancelled));
    }
}

impl Handler<Timeout> for Clocked {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, t: &Timeout) {
        let elapsed = t.now - self.started;
        if self.once.as_ref().unwrap().is(t) {
            assert!(elapsed >= Duration::from_millis(20), "fired early: {elapsed:?}");
            log(&self.log, "once");
        } else if self.repeat.as_ref().unwrap().is(t) {
            self.repeats += 1;
            assert!(elapsed >= Duration::from_millis(5) * self.repeats, "fired early: {elapsed:?}");
            log(&self.log, format!("repeat {}", self.repeats));
            if self.repeats == 5 {
                self.repeat.as_ref().unwrap().unset();
            }
        } else {
            log(&self.log, "unexpected timeout");
        }
    }
}

#[test]
fn timers_one_shot_repeat_and_unset() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    seq.add_service::<TimerServiceTag, _, _>(2, TimerService::new);
    let l2 = l.clone();
    seq.add_actor(1, move || Clocked {
        once: None,
        repeat: None,
        cancelled: None,
        started: Instant::now(),
        repeats: 0,
        log: l2,
    });
    let engine = Engine::start(seq);
    wait_for(&l, "once");
    wait_for(&l, "repeat 5");
    std::thread::sleep(Duration::from_millis(50)); // nothing else must fire
    engine.shutdown(); // with no timer armed but timers alive: hub and service shut down cleanly
    let mut got = lines(&l);
    got.sort();
    assert_eq!(got, vec!["once", "repeat 1", "repeat 2", "repeat 3", "repeat 4", "repeat 5"]);
}

struct NoService {
    log: Log,
}

impl Actor for NoService {
    fn on_start(&mut self, ctx: &mut Context) {
        log(&self.log, format!("{:?}", Timer::new(ctx).err()));
    }
}

#[test]
fn timer_without_service_is_an_error() {
    let _g = serial();
    let l = Log::default();
    let mut seq = StartSequence::new();
    let l2 = l.clone();
    seq.add_actor(1, move || NoService { log: l2 });
    Engine::start(seq).shutdown();
    assert_eq!(lines(&l), vec![format!("{:?}", Some(NoTimerService))]);
}

struct Busy {
    timer: Option<Timer>,
}

impl Actor for Busy {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Timeout, Self>();
        let t = Timer::new(ctx).unwrap();
        t.set_repeat(ctx, Duration::from_micros(100));
        self.timer = Some(t);
    }
}

impl Handler<Timeout> for Busy {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, _t: &Timeout) {}
}

#[test]
fn shutdown_with_armed_timers_on_several_cores() {
    let _g = serial();
    let mut seq = StartSequence::new();
    seq.add_service::<TimerServiceTag, _, _>(3, TimerService::new);
    for core in [1, 2, 3] {
        seq.add_actor(core, || Busy { timer: None });
    }
    let engine = Engine::start(seq);
    std::thread::sleep(Duration::from_millis(30));
    engine.shutdown();
}
