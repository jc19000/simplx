//! PoC of a simplx-like actor engine in Rust.

pub mod affinity;
pub mod bytering;
pub mod engine;
pub mod spsc;
pub mod timer;

pub use engine::{
    Actor, ActorId, ActorRef, Context, CoreId, Engine, Event, Handler, ReferenceError, StartSequence, UndeliveredHandler,
};
pub use timer::{NoTimerService, Timeout, Timer, TimerService, TimerServiceTag};
