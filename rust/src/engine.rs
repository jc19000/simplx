//! simplx-like engine: one pinned, busy-spinning event loop per core, actors owned by their core,
//! and a full mesh of byte rings (one per ordered pair of cores) carrying heterogeneous events.
//!
//! Mapping to simplx:
//!
//! | simplx                                   | here                                         |
//! |------------------------------------------|----------------------------------------------|
//! | `Actor::Event` subclass                  | any `Send + 'static` type                    |
//! | `registerEventHandler<E>(*this)`         | `ctx.register_handler::<E, Self>()`          |
//! | `onEvent(const E&)`                      | `Handler<E>::on_event(&mut self, ctx, src, &E)` |
//! | `Event::Pipe(*this, dst).push<E>(...)`   | `ctx.send(dst, event)`                       |
//! | `registerUndeliveredEventHandler<E>`     | `ctx.register_undelivered_handler::<E, Self>()` |
//! | `throw ReturnToSenderException()`        | `ctx.return_to_sender()`                     |
//! | `newUnreferencedActor<A>()`              | `ctx.new_actor(actor)`                       |
//! | `requestDestroy()` / `onDestroyRequest()` / `acceptDestroy()` | `ctx.request_destroy()` / `on_destroy_request() -> bool` |
//! | `registerCallback(*this)` / `onCallback()` | `ctx.register_callback()` / `on_callback()` |
//! | `addServiceActor<Tag, A>(core)` / `getServiceActorId<Tag>()` | `add_service::<Tag, _>(core, f)` / `ctx.service::<Tag>()` |
//! | `Engine` destructor                      | `Engine::shutdown()` / `Drop`                |
//!
//! Events sent during a loop iteration are staged and published once per destination core at the
//! end of the iteration. Handlers receive a reference into the ring itself: no copy on delivery.
//! On shutdown, regular actors are asked to destroy themselves first, services once no regular
//! actor is left anywhere; the loops exit when no actor is left.

use std::any::TypeId;
use std::collections::HashMap;
use std::mem::{align_of, size_of};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::{self, JoinHandle};

use crate::affinity;
use crate::bytering::{self, Consumer, Producer, RECORD_ALIGN};

pub type CoreId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ActorId {
    pub core: u16,
    pub generation: u16,
    pub index: u32,
}

impl ActorId {
    /// An id no actor ever has; events sent to it come back as undelivered.
    pub const NULL: ActorId = ActorId { core: u16::MAX, generation: 0, index: u32::MAX };
}

pub trait Event: Send + 'static {}
impl<T: Send + 'static> Event for T {}

/// An actor lives on one core for its whole life; it does not need to be `Send`.
pub trait Actor: 'static {
    /// Called once on the actor's core, before any event; register handlers here.
    fn on_start(&mut self, _ctx: &mut Context) {}
    /// Called at the end of the loop iteration in which `register_callback` was called.
    fn on_callback(&mut self, _ctx: &mut Context) {}
    /// Return `false` to postpone destruction. During shutdown the request is repeated on every
    /// loop iteration; otherwise call `request_destroy` again later.
    fn on_destroy_request(&mut self, _ctx: &mut Context) -> bool {
        true
    }
}

pub trait Handler<E: Event>: Actor {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, event: &E);
}

pub trait UndeliveredHandler<E: Event>: Actor {
    fn on_undelivered_event(&mut self, ctx: &mut Context, dst: ActorId, event: &E);
}

// ---- Wire format ---------------------------------------------------------------------------

const KIND_EVENT: u32 = 0;
const KIND_UNDELIVERED: u32 = 1;

/// Record header; the event payload follows at offset `HEADER`. `len` must stay the first field:
/// the byte ring reads it.
#[repr(C)]
struct Header {
    len: u32,
    kind: u32,
    src: ActorId,
    dst: ActorId,
    type_id: TypeId,
    drop_fn: unsafe fn(*mut u8),
}

const HEADER: usize = round_up(size_of::<Header>());

const fn round_up(n: usize) -> usize {
    (n + RECORD_ALIGN - 1) & !(RECORD_ALIGN - 1)
}

const fn record_len<E>() -> usize {
    round_up(HEADER + size_of::<E>())
}

unsafe fn drop_payload<E>(payload: *mut u8) {
    ptr::drop_in_place(payload as *mut E);
}

#[inline]
unsafe fn write_record<E: Event>(record: *mut u8, kind: u32, src: ActorId, dst: ActorId, event: E) {
    const { assert!(align_of::<E>() <= RECORD_ALIGN, "event alignment above 16 is not supported") };
    let header = Header {
        len: record_len::<E>() as u32,
        kind,
        src,
        dst,
        type_id: TypeId::of::<E>(),
        drop_fn: drop_payload::<E>,
    };
    ptr::write(record as *mut Header, header);
    ptr::write(record.add(HEADER) as *mut E, event);
}

/// Growable buffer of records, 16-byte aligned (local events and overflow of full rings).
#[derive(Default)]
struct RecordBuf {
    words: Vec<u128>,
    len: usize,
}

impl RecordBuf {
    #[inline]
    fn alloc(&mut self, n: usize) -> *mut u8 {
        let end = self.len + n;
        let words = end.div_ceil(RECORD_ALIGN);
        if words > self.words.len() {
            self.words.resize(words.max(2 * self.words.len()).max(64), 0);
        }
        let p = unsafe { (self.words.as_mut_ptr() as *mut u8).add(self.len) };
        self.len = end;
        p
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn base(&mut self) -> *mut u8 {
        self.words.as_mut_ptr() as *mut u8
    }

    /// Removes the first `n` bytes.
    fn consume_front(&mut self, n: usize) {
        if n == self.len {
            self.len = 0;
            return;
        }
        let base = self.base();
        unsafe { ptr::copy(base.add(n), base, self.len - n) };
        self.len -= n;
    }
}

// ---- Context ---------------------------------------------------------------------------------

type Thunk = unsafe fn(*mut u8, &mut Context, ActorId, *const u8);

unsafe fn event_thunk<E: Event, A: Handler<E>>(actor: *mut u8, ctx: &mut Context, src: ActorId, event: *const u8) {
    (*(actor as *mut A)).on_event(ctx, src, &*(event as *const E));
}

unsafe fn undelivered_thunk<E: Event, A: UndeliveredHandler<E>>(
    actor: *mut u8,
    ctx: &mut Context,
    dst: ActorId,
    event: *const u8,
) {
    (*(actor as *mut A)).on_undelivered_event(ctx, dst, &*(event as *const E));
}

struct HandlerRegistration {
    index: u32,
    actor_type: TypeId,
    undelivered: bool,
    type_id: TypeId,
    thunk: Thunk,
}

struct Shared {
    shutdown: AtomicBool,
    live_total: AtomicUsize,
    live_regular: AtomicUsize,
    services: HashMap<TypeId, ActorId>,
    cores: usize,
    cores_done: AtomicUsize,
}

#[repr(align(128))]
struct PaddedShared(Shared);

/// Per-core handle given to actors.
pub struct Context {
    core: u16,
    current: ActorId,
    local: RecordBuf,
    outbound: Vec<Option<Producer>>, // indexed by destination core
    overflow: Vec<RecordBuf>,        // records that did not fit in a full ring
    dirty: Vec<CoreId>,
    shared: Arc<PaddedShared>,

    // Effects of the running callback, applied by the event loop once it returns.
    effects: bool,
    return_to_sender: bool,
    registrations: Vec<HandlerRegistration>,
    new_actors: Vec<(ActorId, TypeId, Box<dyn Actor>)>,
    destroy_requests: Vec<ActorId>,
    callbacks: Vec<ActorId>,

    free_slots: Vec<(u32, u16)>,
    next_index: u32,
}

impl Context {
    #[inline]
    pub fn id(&self) -> ActorId {
        self.current
    }

    #[inline]
    pub fn core(&self) -> CoreId {
        self.core as CoreId
    }

    #[inline]
    pub fn send<E: Event>(&mut self, dst: ActorId, event: E) {
        let src = self.current;
        let record = self.alloc_record(dst.core, record_len::<E>());
        unsafe { write_record(record, KIND_EVENT, src, dst, event) };
    }

    /// Event handlers of this actor's type for `E`; call from `on_start` (or later).
    pub fn register_handler<E: Event, A: Handler<E>>(&mut self) {
        self.registrations.push(HandlerRegistration {
            index: self.current.index,
            actor_type: TypeId::of::<A>(),
            undelivered: false,
            type_id: TypeId::of::<E>(),
            thunk: event_thunk::<E, A>,
        });
        self.effects = true;
    }

    pub fn register_undelivered_handler<E: Event, A: UndeliveredHandler<E>>(&mut self) {
        self.registrations.push(HandlerRegistration {
            index: self.current.index,
            actor_type: TypeId::of::<A>(),
            undelivered: true,
            type_id: TypeId::of::<E>(),
            thunk: undelivered_thunk::<E, A>,
        });
        self.effects = true;
    }

    /// From an event handler: send the event back to its sender as undelivered.
    pub fn return_to_sender(&mut self) {
        self.return_to_sender = true;
    }

    /// Creates an actor on this core; it is started once the current callback returns.
    pub fn new_actor<A: Actor>(&mut self, actor: A) -> ActorId {
        let (index, generation) = self.free_slots.pop().unwrap_or_else(|| {
            self.next_index += 1;
            (self.next_index - 1, 0)
        });
        let id = ActorId { core: self.core, generation, index };
        let shared = &self.shared.0;
        shared.live_total.fetch_add(1, Ordering::AcqRel);
        shared.live_regular.fetch_add(1, Ordering::AcqRel);
        self.new_actors.push((id, TypeId::of::<A>(), Box::new(actor)));
        self.effects = true;
        id
    }

    /// `on_destroy_request` of the current actor will be called at the end of this loop iteration.
    pub fn request_destroy(&mut self) {
        self.destroy_requests.push(self.current);
    }

    /// `on_callback` of the current actor will be called at the end of this loop iteration.
    pub fn register_callback(&mut self) {
        self.callbacks.push(self.current);
    }

    pub fn service<Tag: 'static>(&self) -> Option<ActorId> {
        self.shared.0.services.get(&TypeId::of::<Tag>()).copied()
    }

    #[inline]
    fn alloc_record(&mut self, core: u16, len: usize) -> *mut u8 {
        if core == self.core {
            return self.local.alloc(len);
        }
        let c = core as CoreId;
        match self.outbound.get_mut(c).and_then(Option::as_mut) {
            Some(producer) => {
                assert!(len <= producer.max_record(), "event too large for the ring");
                if !self.dirty.contains(&c) {
                    self.dirty.push(c);
                }
                if self.overflow[c].is_empty() {
                    if let Some(record) = producer.reserve(len) {
                        return record;
                    }
                }
                self.overflow[c].alloc(len)
            }
            // Unknown core (e.g. ActorId::NULL): deliver locally, it will bounce as undelivered.
            None => self.local.alloc(len),
        }
    }

    /// Moves an undelivered record back to its sender, without running the event's destructor.
    unsafe fn bounce(&mut self, record: *const u8) {
        let header = &*(record as *const Header);
        if header.kind != KIND_EVENT || header.src == ActorId::NULL {
            (header.drop_fn)(record.add(HEADER) as *mut u8);
            return;
        }
        let len = header.len as usize;
        let src_core = header.src.core;
        // `record` is never in `self.local` (local records are dispatched from a swapped-out
        // buffer), so growing it here cannot move the source.
        let copy = self.alloc_record(src_core, len);
        ptr::copy_nonoverlapping(record, copy, len);
        // Routed to `src`; the receiving loop reads `kind` to know which id is the target.
        (*(copy as *mut Header)).kind = KIND_UNDELIVERED;
    }

    /// Publishes staged records; overflow is retried on the next iteration.
    #[inline]
    fn flush(&mut self) {
        let mut i = 0;
        while i < self.dirty.len() {
            let c = self.dirty[i];
            let producer = self.outbound[c].as_mut().unwrap();
            let overflow = &mut self.overflow[c];
            if !overflow.is_empty() {
                let base = overflow.base();
                let mut offset = 0;
                while offset < overflow.len {
                    let len = unsafe { (base.add(offset) as *const u32).read() } as usize;
                    match producer.reserve(len) {
                        Some(record) => unsafe { ptr::copy_nonoverlapping(base.add(offset), record, len) },
                        None => break,
                    }
                    offset += len;
                }
                overflow.consume_front(offset);
            }
            producer.commit();
            if overflow.is_empty() {
                self.dirty.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

// ---- Start sequence and engine ---------------------------------------------------------------

struct StartEntry {
    core: CoreId,
    service: bool,
    actor_type: TypeId,
    factory: Box<dyn FnOnce() -> Box<dyn Actor> + Send>,
}

pub struct StartSequence {
    entries: Vec<StartEntry>,
    per_core: Vec<u32>,
    used: Vec<bool>,
    services: HashMap<TypeId, ActorId>,
    ring_capacity: usize,
}

impl Default for StartSequence {
    fn default() -> Self {
        Self::new()
    }
}

impl StartSequence {
    pub fn new() -> Self {
        Self { entries: Vec::new(), per_core: Vec::new(), used: Vec::new(), services: HashMap::new(), ring_capacity: 64 * 1024 }
    }

    /// Size in bytes of each inter-core ring.
    pub fn ring_capacity(mut self, bytes: usize) -> Self {
        self.ring_capacity = bytes;
        self
    }

    /// Runs an event loop on `core` even if no start actor is placed there.
    pub fn add_core(&mut self, core: CoreId) {
        if self.per_core.len() <= core {
            self.per_core.resize(core + 1, 0);
            self.used.resize(core + 1, false);
        }
        self.used[core] = true;
    }

    /// Registers an actor built on `core` when the engine starts; returns its id.
    pub fn add_actor<A: Actor, F: FnOnce() -> A + Send + 'static>(&mut self, core: CoreId, factory: F) -> ActorId {
        self.push(core, false, factory)
    }

    /// Same as `add_actor`, and makes the actor reachable through `Context::service::<Tag>()`.
    pub fn add_service<Tag: 'static, A: Actor, F: FnOnce() -> A + Send + 'static>(
        &mut self,
        core: CoreId,
        factory: F,
    ) -> ActorId {
        let id = self.push(core, true, factory);
        assert!(self.services.insert(TypeId::of::<Tag>(), id).is_none(), "service registered twice");
        id
    }

    fn push<A: Actor, F: FnOnce() -> A + Send + 'static>(&mut self, core: CoreId, service: bool, factory: F) -> ActorId {
        self.add_core(core);
        let id = ActorId { core: core as u16, generation: 0, index: self.per_core[core] };
        self.per_core[core] += 1;
        self.entries.push(StartEntry {
            core,
            service,
            actor_type: TypeId::of::<A>(),
            factory: Box::new(move || Box::new(factory()) as Box<dyn Actor>),
        });
        id
    }
}

pub struct Engine {
    shared: Arc<PaddedShared>,
    threads: Vec<JoinHandle<()>>,
}

impl Engine {
    /// Starts one event loop per core of the start sequence; returns once every start actor has
    /// been built and started.
    pub fn start(sequence: StartSequence) -> Engine {
        let StartSequence { entries, per_core, used, services, ring_capacity } = sequence;
        let n = per_core.len();
        let cores: Vec<CoreId> = (0..n).filter(|&c| used[c]).collect();

        let mut outbound: Vec<Vec<Option<Producer>>> = (0..n).map(|_| (0..n).map(|_| None).collect()).collect();
        let mut inbound: Vec<Vec<Consumer>> = (0..n).map(|_| Vec::new()).collect();
        for &src in &cores {
            for &dst in &cores {
                if src != dst {
                    let (p, c) = bytering::channel(ring_capacity);
                    outbound[src][dst] = Some(p);
                    inbound[dst].push(c);
                }
            }
        }

        let regular = entries.iter().filter(|e| !e.service).count();
        let shared = Arc::new(PaddedShared(Shared {
            shutdown: AtomicBool::new(false),
            live_total: AtomicUsize::new(entries.len()),
            live_regular: AtomicUsize::new(regular),
            services,
            cores: cores.len(),
            cores_done: AtomicUsize::new(0),
        }));

        let mut per_core_entries: Vec<Vec<StartEntry>> = (0..n).map(|_| Vec::new()).collect();
        for entry in entries {
            per_core_entries[entry.core].push(entry);
        }

        let started = Arc::new(Barrier::new(cores.len() + 1));
        let mut outbound: Vec<_> = outbound.into_iter().map(Some).collect();
        let mut inbound: Vec<_> = inbound.into_iter().map(Some).collect();
        let mut per_core_entries: Vec<_> = per_core_entries.into_iter().map(Some).collect();
        let threads = cores
            .iter()
            .map(|&core| {
                let loop_state = CoreLoop {
                    core,
                    entries: per_core_entries[core].take().unwrap(),
                    inbound: inbound[core].take().unwrap(),
                    outbound: outbound[core].take().unwrap(),
                    shared: shared.clone(),
                    started: started.clone(),
                };
                thread::Builder::new()
                    .name(format!("engine-core-{core}"))
                    .spawn(move || loop_state.run())
                    .expect("failed to spawn engine thread")
            })
            .collect();
        started.wait();
        Engine { shared, threads }
    }

    /// Requests destruction of every actor (regular actors first, then services) and waits until
    /// all of them accepted and every event loop exited.
    pub fn shutdown(mut self) {
        self.stop();
    }

    fn stop(&mut self) {
        self.shared.0.shutdown.store(true, Ordering::Release);
        for handle in self.threads.drain(..) {
            handle.join().expect("engine thread panicked");
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---- Event loop ------------------------------------------------------------------------------

struct Slot {
    actor: Option<Box<dyn Actor>>,
    actor_ptr: *mut u8,
    actor_type: TypeId,
    generation: u16,
    service: bool,
    destroy_pending: bool,
    callback_iteration: u64,
    handlers: Vec<(TypeId, Thunk)>,
    undelivered: Vec<(TypeId, Thunk)>,
}

impl Slot {
    fn empty() -> Slot {
        Slot {
            actor: None,
            actor_ptr: ptr::null_mut(),
            actor_type: TypeId::of::<()>(),
            generation: 0,
            service: false,
            destroy_pending: false,
            callback_iteration: u64::MAX,
            handlers: Vec::new(),
            undelivered: Vec::new(),
        }
    }
}

#[inline]
fn find(table: &[(TypeId, Thunk)], type_id: TypeId) -> Option<Thunk> {
    table.iter().find(|(t, _)| *t == type_id).map(|(_, thunk)| *thunk)
}

struct CoreLoop {
    core: CoreId,
    entries: Vec<StartEntry>,
    inbound: Vec<Consumer>,
    outbound: Vec<Option<Producer>>,
    shared: Arc<PaddedShared>,
    started: Arc<Barrier>,
}

struct Actors {
    slots: Vec<Slot>,
}

impl Actors {
    #[inline]
    fn live(&self, core: u16, id: ActorId) -> Option<&Slot> {
        if id.core != core {
            return None;
        }
        let slot = self.slots.get(id.index as usize)?;
        (slot.actor.is_some() && slot.generation == id.generation).then_some(slot)
    }

    fn install(&mut self, ctx: &mut Context, id: ActorId, actor_type: TypeId, service: bool, mut actor: Box<dyn Actor>) {
        let index = id.index as usize;
        if self.slots.len() <= index {
            self.slots.resize_with(index + 1, Slot::empty);
        }
        let actor_ptr = &mut *actor as *mut dyn Actor as *mut u8;
        let slot = &mut self.slots[index];
        *slot = Slot { actor: Some(actor), actor_ptr, actor_type, generation: id.generation, service, ..Slot::empty() };
        ctx.current = id;
        let actor: *mut dyn Actor = &mut **self.slots[index].actor.as_mut().unwrap();
        // SAFETY: the box is owned by the slot and stays put while the callback runs (effects that
        // may grow `slots` are applied after it returns).
        unsafe { (*actor).on_start(ctx) };
        self.apply_effects(ctx);
    }

    /// Applies handler registrations and actor creations requested by the callback that just ran.
    #[inline]
    fn apply_effects(&mut self, ctx: &mut Context) {
        if !ctx.effects {
            return;
        }
        ctx.effects = false;
        let current = ctx.current;
        for r in ctx.registrations.drain(..) {
            let slot = &mut self.slots[r.index as usize];
            assert_eq!(slot.actor_type, r.actor_type, "handler registered for another actor type");
            let table = if r.undelivered { &mut slot.undelivered } else { &mut slot.handlers };
            match table.iter_mut().find(|(t, _)| *t == r.type_id) {
                Some(entry) => entry.1 = r.thunk,
                None => table.push((r.type_id, r.thunk)),
            }
        }
        let new_actors = std::mem::take(&mut ctx.new_actors);
        for (id, actor_type, actor) in new_actors {
            self.install(ctx, id, actor_type, false, actor);
        }
        ctx.current = current;
    }

    #[inline]
    unsafe fn dispatch(&mut self, ctx: &mut Context, record: *mut u8) {
        let header = &*(record as *const Header);
        let payload = record.add(HEADER);
        if header.kind == KIND_EVENT {
            let target = self.live(ctx.core, header.dst).map(|s| (s.actor_ptr, find(&s.handlers, header.type_id)));
            if let Some((actor, Some(thunk))) = target {
                ctx.current = header.dst;
                thunk(actor, ctx, header.src, payload);
                let returned = std::mem::take(&mut ctx.return_to_sender);
                self.apply_effects(ctx);
                if !returned {
                    (header.drop_fn)(payload);
                    return;
                }
            }
            ctx.bounce(record);
        } else {
            let target = self.live(ctx.core, header.src).map(|s| (s.actor_ptr, find(&s.undelivered, header.type_id)));
            if let Some((actor, Some(thunk))) = target {
                ctx.current = header.src;
                thunk(actor, ctx, header.dst, payload);
                self.apply_effects(ctx);
            }
            (header.drop_fn)(payload);
        }
    }

    fn run_callbacks(&mut self, ctx: &mut Context, iteration: u64) {
        let callbacks = std::mem::take(&mut ctx.callbacks);
        for id in &callbacks {
            match self.live(ctx.core, *id) {
                Some(slot) if slot.callback_iteration != iteration => {}
                _ => continue, // dead, or already called back in this iteration
            }
            let slot = &mut self.slots[id.index as usize];
            slot.callback_iteration = iteration;
            let actor: *mut dyn Actor = &mut **slot.actor.as_mut().unwrap();
            ctx.current = *id;
            unsafe { (*actor).on_callback(ctx) };
            self.apply_effects(ctx);
        }
        // Keep the allocation for the next iteration.
        let mut callbacks = callbacks;
        callbacks.clear();
        if ctx.callbacks.is_empty() {
            ctx.callbacks = callbacks;
        }
    }

    #[inline]
    fn run_destroy_requests(&mut self, ctx: &mut Context, shared: &Shared) {
        if shared.shutdown.load(Ordering::Relaxed) {
            let services_too = shared.live_regular.load(Ordering::Acquire) == 0;
            for (index, slot) in self.slots.iter_mut().enumerate() {
                if slot.actor.is_some() && !slot.destroy_pending && (!slot.service || services_too) {
                    slot.destroy_pending = true;
                    ctx.destroy_requests.push(ActorId { core: ctx.core, generation: slot.generation, index: index as u32 });
                }
            }
        }
        if ctx.destroy_requests.is_empty() {
            return;
        }
        let requests = std::mem::take(&mut ctx.destroy_requests);
        for id in requests {
            if self.live(ctx.core, id).is_none() {
                continue;
            }
            let index = id.index as usize;
            self.slots[index].destroy_pending = false;
            ctx.current = id;
            let actor: *mut dyn Actor = &mut **self.slots[index].actor.as_mut().unwrap();
            let accepted = unsafe { (*actor).on_destroy_request(ctx) };
            self.apply_effects(ctx);
            if accepted {
                let slot = &mut self.slots[index];
                let service = slot.service;
                let generation = slot.generation;
                *slot = Slot::empty();
                ctx.free_slots.push((id.index, generation.wrapping_add(1)));
                if !service {
                    shared.live_regular.fetch_sub(1, Ordering::AcqRel);
                }
                shared.live_total.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

impl CoreLoop {
    fn run(self) {
        let CoreLoop { core, entries, mut inbound, outbound, shared, started } = self;
        if let Err(e) = affinity::pin_current_thread(core) {
            eprintln!("warning: could not pin engine thread to core {core}: {e}");
        }
        let n = outbound.len();
        let mut ctx = Context {
            core: core as u16,
            current: ActorId::NULL,
            local: RecordBuf::default(),
            outbound,
            overflow: (0..n).map(|_| RecordBuf::default()).collect(),
            dirty: Vec::with_capacity(n),
            shared,
            effects: false,
            return_to_sender: false,
            registrations: Vec::new(),
            new_actors: Vec::new(),
            destroy_requests: Vec::new(),
            callbacks: Vec::new(),
            free_slots: Vec::new(),
            next_index: entries.len() as u32,
        };
        let mut actors = Actors { slots: Vec::new() };

        // Start actors are built and started on their own core, as in simplx.
        for (index, entry) in entries.into_iter().enumerate() {
            let id = ActorId { core: core as u16, generation: 0, index: index as u32 };
            let actor = (entry.factory)();
            actors.install(&mut ctx, id, entry.actor_type, entry.service, actor);
        }
        ctx.flush();
        started.wait();

        let shared = ctx.shared.clone(); // read-only from here on; cloning per iteration would bounce the refcount line
        let mut local = RecordBuf::default();
        let mut iteration: u64 = 0;
        loop {
            for consumer in inbound.iter_mut() {
                let mut any = false;
                while let Some((record, len)) = consumer.peek() {
                    unsafe { actors.dispatch(&mut ctx, record) };
                    consumer.advance(len);
                    any = true;
                }
                if any {
                    consumer.release();
                }
            }
            while !ctx.local.is_empty() {
                std::mem::swap(&mut local, &mut ctx.local);
                let base = local.base();
                let mut offset = 0;
                while offset < local.len {
                    let record = unsafe { base.add(offset) };
                    let len = unsafe { (record as *const u32).read() } as usize;
                    unsafe { actors.dispatch(&mut ctx, record) };
                    offset += len;
                }
                local.len = 0;
            }
            if !ctx.callbacks.is_empty() {
                actors.run_callbacks(&mut ctx, iteration);
            }
            actors.run_destroy_requests(&mut ctx, &shared.0);
            ctx.flush();
            iteration += 1;
            if shared.0.live_total.load(Ordering::Acquire) == 0 && ctx.local.is_empty() {
                break;
            }
        }

        // No actor left anywhere, so nothing can handle an event anymore: drop whatever arrives,
        // push out our remaining overflow, and keep draining until every core did the same.
        let drop_inbound = |inbound: &mut Vec<Consumer>| {
            for consumer in inbound.iter_mut() {
                while let Some((record, len)) = consumer.peek() {
                    let header = unsafe { &*(record as *const Header) };
                    unsafe { (header.drop_fn)(record.add(HEADER)) };
                    consumer.advance(len);
                }
                consumer.release();
            }
        };
        while !ctx.dirty.is_empty() {
            drop_inbound(&mut inbound);
            ctx.flush();
        }
        shared.0.cores_done.fetch_add(1, Ordering::AcqRel);
        while shared.0.cores_done.load(Ordering::Acquire) < shared.0.cores {
            drop_inbound(&mut inbound);
            std::hint::spin_loop();
        }
        drop_inbound(&mut inbound);
    }
}
