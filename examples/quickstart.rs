//! The README example: a ping-pong between core 1 and core 2.
//!
//! cargo run --release --example quickstart

use cannes::{Actor, ActorId, Context, Engine, Handler, StartSequence};

struct Ping(u64);

struct Pong;
impl Actor for Pong {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Ping, Self>();
    }
}
impl Handler<Ping> for Pong {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, ping: &Ping) {
        ctx.send(src, Ping(ping.0 + 1)); // back to the sender, on its own core
    }
}

struct Starter {
    pong: ActorId,
}
impl Actor for Starter {
    fn on_start(&mut self, ctx: &mut Context) {
        ctx.register_handler::<Ping, Self>();
        ctx.send(self.pong, Ping(0));
    }
}
impl Handler<Ping> for Starter {
    fn on_event(&mut self, ctx: &mut Context, src: ActorId, ping: &Ping) {
        if ping.0 < 1_000_000 {
            ctx.send(src, Ping(ping.0 + 1));
        }
    }
}

fn main() {
    let mut sequence = StartSequence::new();
    let pong = sequence.add_actor(2, || Pong); // loop pinned to core 2
    sequence.add_actor(1, move || Starter { pong }); // loop pinned to core 1
    let engine = Engine::start(sequence);
    std::thread::sleep(std::time::Duration::from_secs(1));
    for c in engine.shutdown() {
        // final per-core counters
        println!("core {}: {} events, {}/{} busy loops", c.core, c.events, c.loop_usage, c.loop_total);
    }
}
