//! Thread-to-core pinning through `sched_setaffinity`, without pulling in the libc crate.

use std::io;

const CPU_SET_WORDS: usize = 1024 / 64;

#[repr(C)]
struct CpuSet {
    bits: [u64; CPU_SET_WORDS],
}

extern "C" {
    fn sched_setaffinity(pid: i32, cpusetsize: usize, mask: *const CpuSet) -> i32;
}

pub fn pin_current_thread(core: usize) -> io::Result<()> {
    if core >= CPU_SET_WORDS * 64 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "core id out of range"));
    }
    let mut set = CpuSet { bits: [0; CPU_SET_WORDS] };
    set.bits[core / 64] |= 1 << (core % 64);
    // pid 0 targets the calling thread.
    if unsafe { sched_setaffinity(0, std::mem::size_of::<CpuSet>(), &set) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
