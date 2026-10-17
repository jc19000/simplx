//! Bounded lock-free SPSC ring of variable-size records.
//!
//! Same index scheme as `spsc` (padded `head`/`tail`, cached remote index, batched publish), but
//! slots are bytes: each record is a contiguous, 16-byte aligned block whose first `u32` holds its
//! total length. When a record does not fit before the end of the buffer, a padding record fills
//! the gap and the record starts again at offset 0, so readers always see contiguous memory.

use std::alloc::{self, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const RECORD_ALIGN: usize = 16;
const PAD: u32 = 1 << 31;

#[repr(align(128))]
struct CachePadded<T>(T);

struct Ring {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    capacity: usize,
    buf: *mut u8,
}

// SAFETY: same protocol as `spsc::Ring`; the ring only hands out raw bytes.
unsafe impl Sync for Ring {}
unsafe impl Send for Ring {}

impl Ring {
    fn layout(capacity: usize) -> Layout {
        Layout::from_size_align(capacity, 128).unwrap()
    }
}

impl Drop for Ring {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.buf, Self::layout(self.capacity)) };
    }
}

/// Creates a ring of `capacity` bytes, rounded up to a power of two.
pub fn channel(capacity: usize) -> (Producer, Consumer) {
    let capacity = capacity.max(4 * RECORD_ALIGN).next_power_of_two();
    let buf = unsafe { alloc::alloc(Ring::layout(capacity)) };
    assert!(!buf.is_null(), "ring allocation failed");
    let ring = Arc::new(Ring {
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        capacity,
        buf,
    });
    (Producer { ring: ring.clone(), tail: 0, head_cache: 0 }, Consumer { ring, head: 0, tail_cache: 0 })
}

#[repr(align(128))]
pub struct Producer {
    ring: Arc<Ring>,
    tail: usize,
    head_cache: usize,
}

impl Producer {
    /// Largest record this ring accepts.
    pub fn max_record(&self) -> usize {
        self.ring.capacity / 2
    }

    /// Reserves a record of `len` bytes (a multiple of `RECORD_ALIGN`, at most `max_record()`) and
    /// returns its address, with the length already stored in its first `u32`. The record becomes
    /// visible to the consumer on the next `commit`.
    #[inline]
    pub fn reserve(&mut self, len: usize) -> Option<*mut u8> {
        debug_assert!(len % RECORD_ALIGN == 0 && len >= RECORD_ALIGN && len <= self.max_record());
        let capacity = self.ring.capacity;
        let pos = self.tail & (capacity - 1);
        let pad = if pos + len > capacity { capacity - pos } else { 0 };
        let needed = pad + len;
        if capacity - (self.tail - self.head_cache) < needed {
            self.head_cache = self.ring.head.0.load(Ordering::Acquire);
            if capacity - (self.tail - self.head_cache) < needed {
                return None;
            }
        }
        unsafe {
            if pad != 0 {
                (self.ring.buf.add(pos) as *mut u32).write(pad as u32 | PAD);
                self.tail += pad;
            }
            let record = self.ring.buf.add(self.tail & (capacity - 1));
            (record as *mut u32).write(len as u32);
            self.tail += len;
            Some(record)
        }
    }

    #[inline]
    pub fn commit(&mut self) {
        self.ring.tail.0.store(self.tail, Ordering::Release);
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        self.commit();
    }
}

#[repr(align(128))]
pub struct Consumer {
    ring: Arc<Ring>,
    head: usize,
    tail_cache: usize,
}

impl Consumer {
    /// Returns the next published record (address and length), skipping padding. The record stays
    /// valid until `advance` past it and `release`.
    #[inline]
    pub fn peek(&mut self) -> Option<(*mut u8, usize)> {
        loop {
            if self.head == self.tail_cache {
                self.tail_cache = self.ring.tail.0.load(Ordering::Acquire);
                if self.head == self.tail_cache {
                    return None;
                }
            }
            let record = unsafe { self.ring.buf.add(self.head & (self.ring.capacity - 1)) };
            let len = unsafe { (record as *const u32).read() };
            if len & PAD != 0 {
                self.head += (len & !PAD) as usize;
                continue;
            }
            return Some((record, len as usize));
        }
    }

    #[inline]
    pub fn advance(&mut self, len: usize) {
        self.head += len;
    }

    /// Hands every record read so far back to the producer.
    #[inline]
    pub fn release(&mut self) {
        self.ring.head.0.store(self.head, Ordering::Release);
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    unsafe fn write_record(p: *mut u8, len: usize, value: u64) {
        (p.add(8) as *mut u64).write(value);
        for i in 16..len {
            *p.add(i) = value as u8;
        }
    }

    unsafe fn check_record(p: *const u8, len: usize) -> u64 {
        let value = (p.add(8) as *const u64).read();
        for i in 16..len {
            assert_eq!(*p.add(i), value as u8);
        }
        value
    }

    #[test]
    fn wraps_with_padding() {
        let (mut p, mut c) = channel(256);
        for round in 0..100u64 {
            let len = [16, 48, 112][round as usize % 3];
            let r = p.reserve(len).unwrap();
            unsafe { write_record(r, len, round) };
            p.commit();
            let (r, l) = c.peek().unwrap();
            assert_eq!(l, len);
            assert_eq!(unsafe { check_record(r, l) }, round);
            c.advance(l);
            c.release();
        }
        assert!(c.peek().is_none());
    }

    #[test]
    fn full_ring_rejects() {
        let (mut p, mut c) = channel(256);
        assert!(p.reserve(128).is_some());
        assert!(p.reserve(128).is_some());
        assert!(p.reserve(16).is_none());
        p.commit();
        let (_, l) = c.peek().unwrap();
        c.advance(l);
        c.release();
        assert!(p.reserve(128).is_some());
    }

    #[test]
    fn cross_thread_stress() {
        const N: u64 = 2_000_000;
        let (mut p, mut c) = channel(4096);
        let producer = thread::spawn(move || {
            for i in 0..N {
                let len = 16 * (1 + (i % 7) as usize);
                let r = loop {
                    if let Some(r) = p.reserve(len) {
                        break r;
                    }
                    p.commit();
                };
                unsafe { write_record(r, len, i) };
                if i % 8 == 0 {
                    p.commit();
                }
            }
        });
        let mut expected = 0;
        while expected < N {
            while let Some((r, l)) = c.peek() {
                assert_eq!(l, 16 * (1 + (expected % 7) as usize));
                assert_eq!(unsafe { check_record(r, l) }, expected);
                c.advance(l);
                expected += 1;
            }
            c.release();
        }
        producer.join().unwrap();
    }
}
