/**
 * @file parallel_pthread.h
 * @brief parallel operations as pthread implementation
 * @copyright 2013-2019 Tredzone (www.tredzone.com). All rights reserved.
 * Please see accompanying LICENSE file for licensing terms.
 */
 
#pragma once

#include <pthread.h>

namespace tredzone
{

using mutex_t = pthread_mutex_t;

void mutexDestroy(mutex_t &);
void mutexLock(mutex_t &);
void mutexUnlock(mutex_t &);

inline
void memoryBarrier()
{
    __sync_synchronize();
}

// Relaxed barriers for use in hot paths where full seq_cst is overkill
inline void memoryBarrierAcquire()
{
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}

inline void memoryBarrierRelease()
{
    __atomic_thread_fence(__ATOMIC_RELEASE);
}

// Relaxed CAS — acquire on success, relaxed on failure
template <typename _Type>
inline bool atomicCompareAndSwapAcqRel(_Type *ptr, _Type oldval, _Type newval)
{
    return __atomic_compare_exchange_n(ptr, &oldval, newval, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED);
}

using thread_t = pthread_t;

} // namespace tredzone




 