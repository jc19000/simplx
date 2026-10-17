//! Minimal simplx-like engine: one pinned, busy-spinning event loop per core, actors owned by
//! their core, and a full mesh of SPSC rings (one per ordered pair of cores) for inter-core events.
//!
//! As in simplx, events sent during a loop iteration are staged and published once at the end of
//! the iteration, so a burst of events costs a single Release store per destination core.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use crate::affinity;
use crate::spsc::{self, Consumer, Producer};

pub type CoreId = usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ActorId {
    pub core: u16,
    pub index: u32,
}

/// An actor lives on one core for its whole life; it does not need to be `Send`.
pub trait Actor<M>: 'static {
    fn on_start(&mut self, _ctx: &mut Context<M>) {}
    fn on_event(&mut self, ctx: &mut Context<M>, src: ActorId, event: M);
}

struct Envelope<M> {
    src: ActorId,
    dst: ActorId,
    event: M,
}

type ActorFactory<M> = Box<dyn FnOnce() -> Box<dyn Actor<M>> + Send>;

/// Per-core handle given to actors to send events and stop the engine.
pub struct Context<M> {
    core: u16,
    current: ActorId,
    local: VecDeque<Envelope<M>>,
    outbound: Vec<Option<Producer<Envelope<M>>>>, // indexed by destination core
    overflow: Vec<VecDeque<Envelope<M>>>,         // events that did not fit in a full ring
    dirty: Vec<CoreId>,
    stop: Arc<AtomicBool>,
}

impl<M> Context<M> {
    #[inline]
    pub fn id(&self) -> ActorId {
        self.current
    }

    #[inline]
    pub fn core(&self) -> CoreId {
        self.core as CoreId
    }

    #[inline]
    pub fn send(&mut self, dst: ActorId, event: M) {
        let envelope = Envelope { src: self.current, dst, event };
        if dst.core == self.core {
            self.local.push_back(envelope);
            return;
        }
        let core = dst.core as CoreId;
        let producer = self.outbound[core].as_mut().expect("destination core is not part of the engine");
        if !self.overflow[core].is_empty() {
            self.overflow[core].push_back(envelope);
        } else if let Err(envelope) = producer.write(envelope) {
            self.overflow[core].push_back(envelope);
        }
        if !self.dirty.contains(&core) {
            self.dirty.push(core);
        }
    }

    /// Asks every event loop to exit after its current iteration.
    pub fn stop_engine(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Publishes staged events; events left in overflow are retried on the next iteration.
    #[inline]
    fn flush(&mut self) {
        let mut i = 0;
        while i < self.dirty.len() {
            let core = self.dirty[i];
            let producer = self.outbound[core].as_mut().unwrap();
            let overflow = &mut self.overflow[core];
            while let Some(envelope) = overflow.pop_front() {
                if let Err(envelope) = producer.write(envelope) {
                    overflow.push_front(envelope);
                    break;
                }
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

pub struct StartSequence<M> {
    actors: Vec<(CoreId, ActorFactory<M>)>,
    per_core: Vec<u32>,
    ring_capacity: usize,
}

impl<M: Send + 'static> Default for StartSequence<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + 'static> StartSequence<M> {
    pub fn new() -> Self {
        Self { actors: Vec::new(), per_core: Vec::new(), ring_capacity: 4096 }
    }

    pub fn ring_capacity(mut self, capacity: usize) -> Self {
        self.ring_capacity = capacity;
        self
    }

    /// Registers an actor to be built on `core` when the engine starts and returns its id,
    /// which can be handed to other actors (the equivalent of a simplx service id).
    pub fn add_actor<A, F>(&mut self, core: CoreId, factory: F) -> ActorId
    where
        A: Actor<M>,
        F: FnOnce() -> A + Send + 'static,
    {
        if self.per_core.len() <= core {
            self.per_core.resize(core + 1, 0);
        }
        let id = ActorId { core: core as u16, index: self.per_core[core] };
        self.per_core[core] += 1;
        self.actors.push((core, Box::new(move || Box::new(factory()) as Box<dyn Actor<M>>)));
        id
    }
}

pub struct Engine;

impl Engine {
    /// Starts one event loop per core used by the start sequence and blocks until an actor
    /// calls `Context::stop_engine`.
    pub fn run<M: Send + 'static>(sequence: StartSequence<M>) {
        let StartSequence { actors, per_core, ring_capacity } = sequence;
        let cores: Vec<CoreId> = (0..per_core.len()).filter(|&c| per_core[c] > 0).collect();
        let n = per_core.len();

        // Full mesh: one ring per ordered pair (src, dst).
        let mut outbound: Vec<Vec<Option<Producer<Envelope<M>>>>> = (0..n).map(|_| (0..n).map(|_| None).collect()).collect();
        let mut inbound: Vec<Vec<Consumer<Envelope<M>>>> = (0..n).map(|_| Vec::new()).collect();
        for &src in &cores {
            for &dst in &cores {
                if src != dst {
                    let (p, c) = spsc::channel(ring_capacity);
                    outbound[src][dst] = Some(p);
                    inbound[dst].push(c);
                }
            }
        }

        let mut factories: Vec<Vec<ActorFactory<M>>> = (0..n).map(|_| Vec::new()).collect();
        for (core, factory) in actors {
            factories[core].push(factory);
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut outbound: Vec<_> = outbound.into_iter().map(Some).collect();
        let mut inbound: Vec<_> = inbound.into_iter().map(Some).collect();
        let mut factories: Vec<_> = factories.into_iter().map(Some).collect();
        let handles: Vec<_> = cores
            .iter()
            .map(|&core| {
                let outbound = outbound[core].take().unwrap();
                let inbound = inbound[core].take().unwrap();
                let factories = factories[core].take().unwrap();
                let stop = stop.clone();
                thread::Builder::new()
                    .name(format!("engine-core-{core}"))
                    .spawn(move || event_loop(core, factories, inbound, outbound, stop))
                    .expect("failed to spawn engine thread")
            })
            .collect();
        for handle in handles {
            handle.join().expect("engine thread panicked");
        }
    }
}

fn event_loop<M: 'static>(
    core: CoreId,
    factories: Vec<ActorFactory<M>>,
    mut inbound: Vec<Consumer<Envelope<M>>>,
    outbound: Vec<Option<Producer<Envelope<M>>>>,
    stop: Arc<AtomicBool>,
) {
    if let Err(e) = affinity::pin_current_thread(core) {
        eprintln!("warning: could not pin engine thread to core {core}: {e}");
    }
    let n = outbound.len();
    let mut ctx = Context {
        core: core as u16,
        current: ActorId { core: core as u16, index: 0 },
        local: VecDeque::new(),
        outbound,
        overflow: (0..n).map(|_| VecDeque::new()).collect(),
        dirty: Vec::with_capacity(n),
        stop,
    };

    // Actors are built and started on their own core, as in simplx.
    let mut actors: Vec<Box<dyn Actor<M>>> = factories.into_iter().map(|f| f()).collect();
    for (index, actor) in actors.iter_mut().enumerate() {
        ctx.current = ActorId { core: core as u16, index: index as u32 };
        actor.on_start(&mut ctx);
    }
    ctx.flush();

    let dispatch = |actors: &mut [Box<dyn Actor<M>>], ctx: &mut Context<M>, envelope: Envelope<M>| {
        ctx.current = envelope.dst;
        actors[envelope.dst.index as usize].on_event(ctx, envelope.src, envelope.event);
    };

    while !ctx.stop.load(Ordering::Relaxed) {
        for consumer in inbound.iter_mut() {
            consumer.drain(|envelope| dispatch(&mut actors, &mut ctx, envelope));
        }
        while let Some(envelope) = ctx.local.pop_front() {
            dispatch(&mut actors, &mut ctx, envelope);
        }
        ctx.flush();
    }
}
