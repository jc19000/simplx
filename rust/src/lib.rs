//! PoC of a simplx-like actor engine in Rust.

pub mod affinity;
pub mod bytering;
pub mod engine;
pub mod spsc;

pub use engine::{Actor, ActorId, Context, CoreId, Engine, Event, Handler, StartSequence, UndeliveredHandler};
