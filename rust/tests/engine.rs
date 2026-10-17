//! Engine behaviour tests, one per simplx feature. Each test runs real pinned, spinning event
//! loops, so they are serialized.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use simplx_rs::{Actor, ActorId, Context, Engine, Handler, StartSequence, UndeliveredHandler};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

type Log = Arc<Mutex<Vec<String>>>;

fn log(log: &Log, line: impl Into<String>) {
    log.lock().unwrap().push(line.into());
}

fn wait_for(log: &Log, line: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !log.lock().unwrap().iter().any(|l| l == line) {
        assert!(Instant::now() < deadline, "timed out waiting for {line:?}, log: {:?}", log.lock().unwrap());
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn lines(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

// ---- Heterogeneous events, same core and cross core -----------------------------------------

struct Small(u8);
struct Text(String);
struct Big([u64; 64]);

struct Receiver {
    log: Log,
}

impl Actor for Receiver {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Small, Self>();
        ctx.register_handler::<Text, Self>();
        ctx.register_handler::<Big, Self>();
    }
}

impl Handler<Small> for Receiver {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, e: &Small) {
        log(&self.log, format!("small {} from core {} on core {}", e.0, src.core, ctx.core()));
    }
}

impl Handler<Text> for Receiver {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Text) {
        log(&self.log, format!("text {}", e.0));
    }
}

impl Handler<Big> for Receiver {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Big) {
        log(&self.log, format!("big {}", e.0.iter().sum::<u64>()));
    }
}

struct Sender {
    remote: ActorId,
    log: Log,
}

impl Actor for Sender {
    fn on_start(&mut self, ctx: &mut Context) {
        let local = ctx.new_actor(Receiver { log: self.log.clone() });
        for dst in [local, self.remote] {
            ctx.send(dst, Small(7));
            ctx.send(dst, Text("hello".to_string()));
            ctx.send(dst, Big([1; 64]));
        }
    }
}

#[test]
fn heterogeneous_events_local_and_remote() {
    let _g = serial();
    let log = Log::default();
    let mut seq = StartSequence::new();
    let l = log.clone();
    let remote = seq.add_actor(2, move || Receiver { log: l });
    let l = log.clone();
    seq.add_actor(1, move || Sender { remote, log: l });
    let engine = Engine::start(seq);
    wait_for(&log, "big 64");
    let deadline = Instant::now() + Duration::from_secs(10);
    while lines(&log).len() < 6 {
        assert!(Instant::now() < deadline);
    }
    engine.shutdown();
    let mut got = lines(&log);
    got.sort();
    assert_eq!(
        got,
        vec!["big 64", "big 64", "small 7 from core 1 on core 1", "small 7 from core 1 on core 2", "text hello", "text hello"]
    );
}

// ---- Undelivered events ----------------------------------------------------------------------

struct Ping;
struct Unhandled;

struct Bouncer;

impl Actor for Bouncer {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Ping, Self>();
    }
}

impl Handler<Ping> for Bouncer {
    fn on_event(&mut self, ctx: &mut Context, _src: ActorId, _e: &Ping) {
        ctx.return_to_sender();
    }
}

struct Doomed;
impl Actor for Doomed {}

struct UndeliveredSender {
    bouncer: ActorId,
    log: Log,
}

impl Actor for UndeliveredSender {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_undelivered_handler::<Ping, Self>();
        ctx.register_undelivered_handler::<Unhandled, Self>();
        ctx.send(self.bouncer, Unhandled); // no handler registered for it
        ctx.send(self.bouncer, Ping); // handler returns it to sender
        ctx.send(ActorId::NULL, Ping); // no such actor
        let doomed = ctx.new_actor(Doomed);
        ctx.send(doomed, Ping); // actor has no handler at all
    }
}

impl UndeliveredHandler<Ping> for UndeliveredSender {
    fn on_undelivered_event(&mut self, _ctx: &mut Context, dst: ActorId, _e: &Ping) {
        let what = if dst == ActorId::NULL { "null" } else if dst == self.bouncer { "bouncer" } else { "doomed" };
        log(&self.log, format!("undelivered ping to {what}"));
    }
}

impl UndeliveredHandler<Unhandled> for UndeliveredSender {
    fn on_undelivered_event(&mut self, _ctx: &mut Context, _dst: ActorId, _e: &Unhandled) {
        log(&self.log, "undelivered unhandled");
    }
}

#[test]
fn undelivered_events_come_back_to_sender() {
    let _g = serial();
    let log = Log::default();
    let mut seq = StartSequence::new();
    let bouncer = seq.add_actor(2, || Bouncer);
    let l = log.clone();
    seq.add_actor(1, move || UndeliveredSender { bouncer, log: l });
    let engine = Engine::start(seq);
    let deadline = Instant::now() + Duration::from_secs(10);
    while lines(&log).len() < 4 {
        assert!(Instant::now() < deadline, "{:?}", lines(&log));
    }
    engine.shutdown();
    let mut got = lines(&log);
    got.sort();
    assert_eq!(
        got,
        vec!["undelivered ping to bouncer", "undelivered ping to doomed", "undelivered ping to null", "undelivered unhandled"]
    );
}

// ---- Lifecycle: dynamic actors, destroy protocol, callbacks ----------------------------------

struct Stubborn {
    refusals: u32,
    log: Log,
}

impl Actor for Stubborn {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.request_destroy();
    }

    fn on_destroy_request(&mut self, ctx: &mut Context) -> bool {
        if self.refusals > 0 {
            self.refusals -= 1;
            ctx.register_callback(); // retry from the callback, next iteration
            return false;
        }
        log(&self.log, "stubborn destroyed");
        true
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        log(&self.log, "callback");
        ctx.request_destroy();
    }
}

struct Spawner {
    log: Log,
}

impl Actor for Spawner {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.new_actor(Stubborn { refusals: 3, log: self.log.clone() });
    }
}

#[test]
fn destroy_protocol_and_callbacks() {
    let _g = serial();
    let log = Log::default();
    let mut seq = StartSequence::new();
    let l = log.clone();
    seq.add_actor(1, move || Spawner { log: l });
    let engine = Engine::start(seq);
    wait_for(&log, "stubborn destroyed");
    engine.shutdown();
    assert_eq!(lines(&log), vec!["callback", "callback", "callback", "stubborn destroyed"]);
}

// ---- Services and shutdown order -------------------------------------------------------------

struct ServiceTag;

struct Service {
    log: Log,
}

impl Actor for Service {
    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        log(&self.log, "service destroyed");
        true
    }
}

struct Client {
    pending: u32,
    log: Log,
}

impl Actor for Client {
    fn on_start(&mut self, ctx: &mut Context) {
        assert!(ctx.service::<ServiceTag>().is_some());
        assert!(ctx.service::<Client>().is_none());
    }

    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        // Postpone a few times, as simplx's sync-exit tutorial does.
        if self.pending > 0 {
            self.pending -= 1;
            return false;
        }
        log(&self.log, "client destroyed");
        true
    }
}

#[test]
fn shutdown_destroys_regular_actors_before_services() {
    let _g = serial();
    let log = Log::default();
    let mut seq = StartSequence::new();
    let l = log.clone();
    seq.add_service::<ServiceTag, _, _>(1, move || Service { log: l });
    for core in [1, 2] {
        let l = log.clone();
        seq.add_actor(core, move || Client { pending: 100, log: l });
    }
    Engine::start(seq).shutdown();
    assert_eq!(lines(&log), vec!["client destroyed", "client destroyed", "service destroyed"]);
}

// ---- Full rings and event drop accounting ----------------------------------------------------

struct Counted(#[allow(dead_code)] Arc<()>, u64);

struct Sink {
    next: u64,
    received: Arc<AtomicUsize>,
}

impl Actor for Sink {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Counted, Self>();
    }
}

impl Handler<Counted> for Sink {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Counted) {
        assert_eq!(e.1, self.next, "events reordered");
        self.next += 1;
        self.received.fetch_add(1, Ordering::Relaxed);
    }
}

struct Flood {
    sink: ActorId,
    token: Arc<()>,
    count: u64,
}

impl Actor for Flood {
    fn on_start(&mut self, ctx: &mut Context) {
        // Far more than the 4 KiB ring holds in one iteration: exercises the overflow path.
        for i in 0..self.count {
            ctx.send(self.sink, Counted(self.token.clone(), i));
        }
    }
}

#[test]
fn full_ring_overflows_in_order_and_every_event_is_dropped() {
    let _g = serial();
    const N: u64 = 100_000;
    let token = Arc::new(());
    let received = Arc::new(AtomicUsize::new(0));
    let mut seq = StartSequence::new().ring_capacity(4096);
    let r = received.clone();
    let sink = seq.add_actor(2, move || Sink { next: 0, received: r });
    let t = token.clone();
    seq.add_actor(1, move || Flood { sink, token: t, count: N });
    let engine = Engine::start(seq);
    let deadline = Instant::now() + Duration::from_secs(10);
    while received.load(Ordering::Relaxed) < N as usize {
        assert!(Instant::now() < deadline, "received {}", received.load(Ordering::Relaxed));
    }
    engine.shutdown();
    assert_eq!(Arc::strong_count(&token), 1, "some events were leaked");
}

#[test]
fn events_in_flight_at_shutdown_are_dropped() {
    let _g = serial();
    let token = Arc::new(());
    let received = Arc::new(AtomicUsize::new(0));
    let mut seq = StartSequence::new().ring_capacity(4096);
    let r = received.clone();
    let sink = seq.add_actor(2, move || Sink { next: 0, received: r });
    let t = token.clone();
    seq.add_actor(1, move || Flood { sink, token: t, count: 50_000 });
    Engine::start(seq).shutdown(); // immediately, while the flood is still being delivered
    assert_eq!(Arc::strong_count(&token), 1, "some events were leaked");
}

// ---- Manual flush and counters ---------------------------------------------------------------

struct Mark(Instant);

struct Stamp {
    log: Log,
}

impl Actor for Stamp {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Mark, Self>();
    }
}

impl Handler<Mark> for Stamp {
    fn on_event(&mut self, _ctx: &mut Context, _src: ActorId, e: &Mark) {
        log(&self.log, format!("{}", e.0.elapsed().as_millis()));
    }
}

/// Sends a Mark, optionally flushes, then keeps its core busy for 40 ms. Done from a callback:
/// `on_start` runs before the other cores enter their event loop.
struct SlowSender {
    dst: ActorId,
    flush: bool,
}

impl Actor for SlowSender {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_callback();
    }

    fn on_callback(&mut self, ctx: &mut Context) {
        ctx.send(self.dst, Mark(Instant::now()));
        if self.flush {
            ctx.flush();
        }
        let until = Instant::now() + Duration::from_millis(40);
        while Instant::now() < until {
            std::hint::spin_loop();
        }
    }
}

fn mark_delay_ms(flush: bool) -> u128 {
    let log = Log::default();
    let mut seq = StartSequence::new();
    let l = log.clone();
    let dst = seq.add_actor(2, move || Stamp { log: l });
    seq.add_actor(1, move || SlowSender { dst, flush });
    let engine = Engine::start(seq);
    let deadline = Instant::now() + Duration::from_secs(10);
    while lines(&log).is_empty() {
        assert!(Instant::now() < deadline);
    }
    engine.shutdown();
    lines(&log)[0].parse().unwrap()
}

#[test]
fn manual_flush_publishes_before_the_end_of_the_iteration() {
    let _g = serial();
    let without = mark_delay_ms(false);
    let with = mark_delay_ms(true);
    assert!(without >= 40, "without flush the event waits for the end of the handler: {without} ms");
    assert!(with < 20, "with flush the event is delivered while the sender is busy: {with} ms");
}

struct Echo;

impl Actor for Echo {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Small, Self>();
    }
}

impl Handler<Small> for Echo {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, e: &Small) {
        ctx.send(src, Small(e.0));
    }
}

struct Pinger {
    echo: ActorId,
    left: u32,
    done: Arc<AtomicUsize>,
}

impl Actor for Pinger {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Small, Self>();
        ctx.send(self.echo, Small(0));
    }
}

impl Handler<Small> for Pinger {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, _e: &Small) {
        self.left -= 1;
        if self.left == 0 {
            self.done.store(1, Ordering::Release);
        } else {
            ctx.send(src, Small(0));
        }
    }
}

#[test]
fn counters_account_for_events_bytes_and_publications() {
    let _g = serial();
    const N: u64 = 1000;
    let done = Arc::new(AtomicUsize::new(0));
    let mut seq = StartSequence::new();
    let echo = seq.add_actor(2, || Echo);
    let d = done.clone();
    seq.add_actor(1, move || Pinger { echo, left: N as u32, done: d });
    let engine = Engine::start(seq);
    let deadline = Instant::now() + Duration::from_secs(10);
    while done.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
    }
    let counters = engine.shutdown();
    assert_eq!(counters.iter().map(|c| c.core).collect::<Vec<_>>(), vec![1, 2]);
    let record = 64; // 48-byte header + 1-byte event, rounded to 16
    for c in &counters {
        assert_eq!(c.events, N, "core {}", c.core);
        assert_eq!(c.callbacks, 0);
        assert!(c.loop_usage >= N && c.loop_usage <= c.loop_total, "{c:?}");
        let other = if c.core == 1 { 2 } else { 1 };
        assert_eq!(c.bytes_written_to[other], N * record, "{c:?}");
        assert_eq!(c.bytes_written_to[c.core], 0);
        // One event in flight at a time: one publication per event sent.
        assert!(c.publications >= N - 1 && c.publications <= N, "{c:?}");
    }
}
