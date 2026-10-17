//! Bounded lock-free single-producer / single-consumer ring.
//!
//! - `head` is written only by the consumer, `tail` only by the producer; each lives on its own
//!   cache line pair so the two sides never false-share.
//! - Each side keeps a local copy of the other side's index and only reloads it when the ring
//!   looks full (producer) or empty (consumer), which keeps cache-line traffic to a minimum.
//! - Writes and reads can be batched: `write` stages an item and `commit` publishes all staged
//!   items with a single Release store (same on the consumer side with `read` / `release`).
//! - `Producer` and `Consumer` are themselves cache-line aligned: their local indexes are written
//!   on every operation and must not share a line with the other side's state, wherever the
//!   containers holding them were allocated.
//! - Splitting the ring into a non-clonable `Producer` and `Consumer` makes the single-writer /
//!   single-reader contract a compile-time guarantee.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(align(128))]
struct CachePadded<T>(T);

struct Ring<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    mask: usize,
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: a slot is only ever accessed by the producer before it is published and by the consumer
// after it was published and before it is released; the Release/Acquire pairs on head and tail
// order those accesses.
unsafe impl<T: Send> Sync for Ring<T> {}
unsafe impl<T: Send> Send for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let head = *self.head.0.get_mut();
        let tail = *self.tail.0.get_mut();
        for i in head..tail {
            unsafe { (*self.buf[i & self.mask].get()).assume_init_drop() };
        }
    }
}

/// Creates a ring holding up to `capacity` items, rounded up to a power of two.
pub fn channel<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let capacity = capacity.max(2).next_power_of_two();
    let buf = (0..capacity).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
    let ring = Arc::new(Ring {
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        mask: capacity - 1,
        buf,
    });
    (Producer { ring: ring.clone(), tail: 0, head_cache: 0 }, Consumer { ring, head: 0, tail_cache: 0 })
}

/// Writing end of a typed SPSC ring (one thread).
#[repr(align(128))]
pub struct Producer<T> {
    ring: Arc<Ring<T>>,
    tail: usize,
    head_cache: usize,
}

impl<T> Producer<T> {
    /// Stages `value`; it becomes visible to the consumer on the next `commit`.
    #[inline]
    pub fn write(&mut self, value: T) -> Result<(), T> {
        let capacity = self.ring.mask + 1;
        if self.tail - self.head_cache == capacity {
            self.head_cache = self.ring.head.0.load(Ordering::Acquire);
            if self.tail - self.head_cache == capacity {
                return Err(value);
            }
        }
        unsafe { (*self.ring.buf[self.tail & self.ring.mask].get()).write(value) };
        self.tail += 1;
        Ok(())
    }

    /// Publishes every staged item with a single Release store.
    #[inline]
    pub fn commit(&mut self) {
        self.ring.tail.0.store(self.tail, Ordering::Release);
    }

    /// `write` then `commit`; gives the value back if the ring is full.
    #[inline]
    pub fn push(&mut self, value: T) -> Result<(), T> {
        self.write(value)?;
        self.commit();
        Ok(())
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        self.commit(); // staged items are dropped by the ring
    }
}

/// Reading end of a typed SPSC ring (one thread).
#[repr(align(128))]
pub struct Consumer<T> {
    ring: Arc<Ring<T>>,
    head: usize,
    tail_cache: usize,
}

impl<T> Consumer<T> {
    /// Takes the next published item; its slot is handed back to the producer on `release`.
    #[inline]
    pub fn read(&mut self) -> Option<T> {
        if self.head == self.tail_cache {
            self.tail_cache = self.ring.tail.0.load(Ordering::Acquire);
            if self.head == self.tail_cache {
                return None;
            }
        }
        let value = unsafe { (*self.ring.buf[self.head & self.ring.mask].get()).assume_init_read() };
        self.head += 1;
        Some(value)
    }

    /// Hands the slots of every item read so far back to the producer.
    #[inline]
    pub fn release(&mut self) {
        self.ring.head.0.store(self.head, Ordering::Release);
    }

    /// `read` then `release`.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        let value = self.read()?;
        self.release();
        Some(value)
    }

    /// Hands every item published so far to `f`, then releases their slots at once.
    /// Returns the number of items processed.
    #[inline]
    pub fn drain(&mut self, mut f: impl FnMut(T)) -> usize {
        self.tail_cache = self.ring.tail.0.load(Ordering::Acquire);
        let n = self.tail_cache - self.head;
        if n == 0 {
            return 0;
        }
        while self.head != self.tail_cache {
            let value = unsafe { (*self.ring.buf[self.head & self.ring.mask].get()).assume_init_read() };
            self.head += 1;
            f(value);
        }
        self.release();
        n
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.release(); // items already read must not be dropped again by the ring
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn fifo_and_capacity() {
        let (mut p, mut c) = channel::<u32>(4);
        for i in 0..4 {
            p.push(i).unwrap();
        }
        assert_eq!(p.push(4), Err(4));
        assert_eq!(c.pop(), Some(0));
        p.push(4).unwrap();
        let mut got = vec![];
        c.drain(|v| got.push(v));
        assert_eq!(got, vec![1, 2, 3, 4]);
        assert_eq!(c.pop(), None);
    }

    #[test]
    fn staged_items_invisible_until_commit() {
        let (mut p, mut c) = channel::<u32>(8);
        p.write(1).unwrap();
        assert_eq!(c.pop(), None);
        p.commit();
        assert_eq!(c.pop(), Some(1));
    }

    #[test]
    fn drops_remaining_items_once() {
        let marker = Arc::new(());
        {
            let (mut p, mut c) = channel(8);
            for _ in 0..5 {
                p.push(marker.clone()).unwrap();
            }
            drop(c.read()); // read but not released
            assert_eq!(Arc::strong_count(&marker), 5);
        }
        assert_eq!(Arc::strong_count(&marker), 1);
    }

    #[test]
    fn cross_thread_stress() {
        const N: u64 = 5_000_000;
        let (mut p, mut c) = channel::<u64>(1024);
        let producer = thread::spawn(move || {
            for i in 0..N {
                let mut v = i;
                while let Err(back) = p.write(v) {
                    p.commit();
                    v = back;
                    std::hint::spin_loop();
                }
                if i % 16 == 0 {
                    p.commit();
                }
            }
        });
        let mut expected = 0;
        while expected < N {
            c.drain(|v| {
                assert_eq!(v, expected);
                expected += 1;
            });
        }
        producer.join().unwrap();
    }
}
