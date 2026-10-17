//! Thread-to-core pinning through `sched_setaffinity` (Linux), without pulling in the libc crate.
//! On other systems pinning is reported as unsupported and engine threads run unpinned.

use std::io;

#[cfg(target_os = "linux")]
mod imp {
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
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use std::io;

    pub fn pin_current_thread(_core: usize) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "thread pinning is only implemented on Linux"))
    }
}

/// Pins the calling thread to `core`.
pub fn pin_current_thread(core: usize) -> io::Result<()> {
    imp::pin_current_thread(core)
}
