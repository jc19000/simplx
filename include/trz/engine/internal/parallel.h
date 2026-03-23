/**
 * @file parallel.h
 * @brief low-level read/write operations
 * @copyright 2013-2019 Tredzone (www.tredzone.com). All rights reserved.
 * Please see accompanying LICENSE file for licensing terms.
 */

#pragma once

#include <bitset>
#include <csignal>
#include <limits>

#include "trz/engine/internal/thread.h"

#include "trz/engine/platform.h"

namespace tredzone
{

// Number of spin iterations (with pause/yield hint) before falling back to sched_yield().
// Tunable: higher = better latency under contention, more CPU burn.
#ifndef TREDZONE_SPIN_COUNT_BEFORE_YIELD
#define TREDZONE_SPIN_COUNT_BEFORE_YIELD 128
#endif

template <class _NodesHandle> class Parallel
{
  public:
    typedef typename _NodesHandle::NodeId NodeId;
    typedef typename _NodesHandle::Shared Shared;
    struct NodeInUseException;
    class Node;
    template <class _NodesHandleInit> Parallel(const _NodesHandleInit &);
    ~Parallel();
    inline size_t size() const { return (size_t)nodesHandle.size; }

  protected:
    _NodesHandle nodesHandle;

  private:
    typedef typename _NodesHandle::NodeHandle NodeHandle;
    typedef typename _NodesHandle::ReaderSharedHandle ReaderSharedHandle;
    typedef typename _NodesHandle::WriterSharedHandle WriterSharedHandle;

    inline void activateNode(NodeId); // throw (NodeInUseException)
};

template <class _NodesHandle> struct Parallel<_NodesHandle>::NodeInUseException : std::exception
{
    virtual const char *what() const noexcept { return "tredzone::Parallel::NodeInUseException"; }
};

template <class _NodesHandle>
class Parallel<_NodesHandle>::Node
{
  public:
    typedef std::bitset<_NodesHandle::MAX_SIZE> WriteSignalBitSet;

    const NodeId id; // starts at 0

    inline Node(Parallel &, NodeId); // throw (NodeInUseException)
    inline ~Node();
    inline void synchronize();
    inline void synchronizePreBarrier();
    inline void synchronizePostBarrier();
    inline Shared &getReferenceToWriterShared(NodeId);
    inline void setWriteSignal(NodeId, bool flag = true);
    inline void flushWritesToPeers();
    inline size_t getNodeCount() const noexcept;

  protected:
    NodeHandle &nodeHandle;

  private:
    enum ReaderCASEnum
    {
        READER_CAS_IDLE = 0,
        READER_CAS_ON,
        READER_CAS_OFF
    };
    struct ForEachConstructorReadOperator
    {
        inline void operator()(ReaderSharedHandle &sharedHandle, NodeId)
        {
            assert(!sharedHandle.getIsReaderActive());
            sharedHandle.setIsReaderActive(true);
            int spins = 0;
            while (!atomicCompareAndSwapAcqRel<sig_atomic_t>(
                     &sharedHandle.getReferenceToReaderCAS(), READER_CAS_IDLE, READER_CAS_ON))
            {
                if (++spins < TREDZONE_SPIN_COUNT_BEFORE_YIELD)
                {
#if defined(__aarch64__)
                    __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__) || defined(__i386__)
                    __asm__ volatile("pause" ::: "memory");
#endif
                }
                else
                {
                    Thread::sleep();
                    spins = 0;
                }
            }
        }
    };
    struct ForEachDestructorReadOperator
    {
        inline void operator()(ReaderSharedHandle &sharedHandle, NodeId)
        {
            assert(sharedHandle.getIsReaderActive());
            assert(sharedHandle.getReferenceToReaderCAS() == READER_CAS_ON);
            atomicCompareAndSwapAcqRel<sig_atomic_t>(&sharedHandle.getReferenceToReaderCAS(), READER_CAS_ON, READER_CAS_IDLE);
            sharedHandle.setIsReaderActive(false);
        }
    };
    struct ForEachConstructorWriteOperator
    {
        inline void operator()(WriterSharedHandle &sharedHandle, NodeId)
        {
            assert(!sharedHandle.getIsWriteLocked());
            sharedHandle.setIsWriterActive(true);
        }
    };
    struct ForEachDestructorWriteOperator
    {
        inline void operator()(WriterSharedHandle &sharedHandle, NodeId)
        {
            sharedHandle.setIsWriterActive(false);
            int spins = 0;
            while (sharedHandle.getIsWriteLocked())
            {
                if (atomicCompareAndSwapAcqRel<sig_atomic_t>(&sharedHandle.getReferenceToReaderCAS(), READER_CAS_IDLE,
                                                       READER_CAS_OFF))
                {
                    sharedHandle.setIsWriteLocked(false);
                    atomicCompareAndSwapAcqRel<sig_atomic_t>(&sharedHandle.getReferenceToReaderCAS(), READER_CAS_OFF,
                                                       READER_CAS_IDLE);
                }
                if (++spins < TREDZONE_SPIN_COUNT_BEFORE_YIELD)
                {
#if defined(__aarch64__)
                    __asm__ volatile("yield" ::: "memory");
#elif defined(__x86_64__) || defined(__i386__)
                    __asm__ volatile("pause" ::: "memory");
#endif
                }
                else
                {
                    Thread::sleep();
                    spins = 0;
                }
            }
        }
    };
    struct ForEachSynchronizeReadOperator
    {
        Node &node;
        NodeId nodes[_NodesHandle::MAX_SIZE];
        int nodesCount;
        bool checkWriteFailed;
        inline ForEachSynchronizeReadOperator(Node &pnode) : node(pnode), nodesCount(-1), checkWriteFailed(false) {}
        inline void operator()(ReaderSharedHandle &sharedHandle, NodeId writerNodeId)
        {
            assert(sharedHandle.getIsReaderActive());
            assert(sharedHandle.getReferenceToReaderCAS() == READER_CAS_ON);
            if (sharedHandle.getIsWriteLocked())
            {
                sharedHandle.read();
                assert(nodesCount + 1 < (int)node.nodesHandleSize);
                nodes[++nodesCount] = writerNodeId;
            }
            checkWriteFailed =
                checkWriteFailed || (node.writeActivity[writerNodeId] && !sharedHandle.getIsWriterActive());
        }
        inline void reset()
        {
            nodesCount = -1;
            checkWriteFailed = false;
        }
    };
    struct ForEachSynchronizeWriteFailedOperator
    {
        WriteSignalBitSet &writeActivity;
        inline ForEachSynchronizeWriteFailedOperator(Node &node) : writeActivity(node.writeActivity) {}
        inline void operator()(WriterSharedHandle &sharedHandle, NodeId readerNodeId)
        {
            if (!sharedHandle.getIsReaderActive())
            {
                if (sharedHandle.getIsWriteLocked())
                {
                    if (atomicCompareAndSwapAcqRel<sig_atomic_t>(&sharedHandle.getReferenceToReaderCAS(), READER_CAS_IDLE,
                                                           READER_CAS_OFF))
                    {
                        sharedHandle.writeFailed();
                        sharedHandle.setIsWriteLocked(false);
                        writeActivity[readerNodeId] = false;
                        atomicCompareAndSwapAcqRel<sig_atomic_t>(&sharedHandle.getReferenceToReaderCAS(), READER_CAS_OFF,
                                                           READER_CAS_IDLE);
                    }
                }
                else
                {
                    writeActivity[readerNodeId] = false;
                }
            }
        }
    };
    struct SynchronizeWriteOperator
    {
        NodeId nodes[_NodesHandle::MAX_SIZE];
        int nodesCount;
        inline SynchronizeWriteOperator() {}
        inline SynchronizeWriteOperator(Node &node) { reset(node); }
        inline void reset(Node &node)
        {
            nodesCount = -1;
            for (unsigned i = 0; i < node.nodesHandleSize; ++i)
            {
                node.writeSignal[node.id] = false; // Local writes need to be processed separately, reset false at each
                                                   // iteration as it may change during write or writeFailed
                typename WriteSignalBitSet::reference iwriteSignal = node.writeSignal[i];
                WriterSharedHandle *sharedHandle;
                if (iwriteSignal &&
                    !(sharedHandle = &node.nodeHandle.getWriterSharedHandle((NodeId)i))->getIsWriteLocked())
                {
                    assert(node.id != (NodeId)i);
                    iwriteSignal = false;
                    if (sharedHandle->getIsReaderActive())
                    {
                        if (sharedHandle->write())
                        {
                            nodes[++nodesCount] = (NodeId)i;
                            node.writeActivity[i] = true;
                        }
                    }
                    else
                    {
                        sharedHandle->writeFailed();
                    }
                }
            }
        }
    };
    _NodesHandle &nodesHandle;
    const size_t nodesHandleSize;
    WriteSignalBitSet writeSignal;
    WriteSignalBitSet writeActivity;
    ForEachSynchronizeReadOperator readOperator;
    SynchronizeWriteOperator writeOperator;
};

template <class _NodesHandle>
template <class _NodesHandleInit>
Parallel<_NodesHandle>::Parallel(const _NodesHandleInit &nodesHandleInit) : nodesHandle(nodesHandleInit)
{
    assert(_NodesHandle::MAX_SIZE <= std::numeric_limits<NodeId>::max());
}

template <class _NodesHandle> Parallel<_NodesHandle>::~Parallel()
{
    for (NodeId i = 0; i < nodesHandle.size; ++i)
    {
        for (; nodesHandle.isNodeActive(i); Thread::sleep())
        {
        }
    }
}

template <class _NodesHandle> void Parallel<_NodesHandle>::activateNode(NodeId nodeId)
{
    if (!nodesHandle.activateNode(nodeId))
    {
        throw NodeInUseException();
    }
}

template <class _NodesHandle>
Parallel<_NodesHandle>::Node::Node(Parallel &parallel, NodeId pid)
    : id((parallel.activateNode(pid), pid)), nodeHandle(parallel.nodesHandle.getNodeHandle(id)),
      nodesHandle(parallel.nodesHandle), nodesHandleSize(nodesHandle.size), readOperator(*this)
{
    assert(id < nodesHandleSize);

    ForEachConstructorWriteOperator writeOperator;
    nodeHandle.foreachWrite(id, writeOperator);

    memoryBarrier(); // set isWriterActive first by writeOperator to avoid false detection of synchronizeWriteFailed by
                     // peer nodes

    ForEachConstructorReadOperator readOperator;
    nodeHandle.foreachRead(id, readOperator);
}

template <class _NodesHandle> Parallel<_NodesHandle>::Node::~Node()
{
    ForEachDestructorReadOperator readOperator;
    nodeHandle.foreachRead(id, readOperator);

    memoryBarrier(); // update readerCAS first by readOperator before unset isWriterActive by writeOperator to avoid
                     // concurrent modify of readerCAS during synchronizeWriteFailed by peer nodes

    for (; writeSignal.any(); Thread::sleep())
    {
        SynchronizeWriteOperator writeOperator(*this);
        memoryBarrierRelease();
        for (int i = writeOperator.nodesCount; i > -1; --i)
        {
            assert(!nodeHandle.getWriterSharedHandle(writeOperator.nodes[i]).getIsWriteLocked());
            nodeHandle.getWriterSharedHandle(writeOperator.nodes[i]).setIsWriteLocked(true);
        }
        ForEachSynchronizeWriteFailedOperator writeFailedOperator(*this);
        nodeHandle.foreachWrite(id, writeFailedOperator);
    }
    ForEachDestructorWriteOperator writeOperator;
    nodeHandle.foreachWrite(id, writeOperator);
    nodesHandle.deactivateNode(id);
}

template <class _NodesHandle> void Parallel<_NodesHandle>::Node::synchronize()
{
    synchronizePreBarrier();
    synchronizePostBarrier();
}

template <class _NodesHandle> void Parallel<_NodesHandle>::Node::synchronizePreBarrier()
{
    readOperator.reset();
    nodeHandle.foreachRead(id, readOperator);
    writeOperator.reset(*this);
}

template <class _NodesHandle> void Parallel<_NodesHandle>::Node::synchronizePostBarrier()
{
    // Release barrier: ensure all reads from synchronizePreBarrier are visible
    // before we unlock write locks. Full seq_cst is overkill here.
    memoryBarrierRelease();

    for (int i = readOperator.nodesCount; i > -1; --i)
    {
        assert(nodeHandle.getReaderSharedHandle(readOperator.nodes[i]).getIsWriteLocked());
        nodeHandle.getReaderSharedHandle(readOperator.nodes[i]).setIsWriteLocked(false);
    }
    for (int i = writeOperator.nodesCount; i > -1; --i)
    {
        assert(!nodeHandle.getWriterSharedHandle(writeOperator.nodes[i]).getIsWriteLocked());
        nodeHandle.getWriterSharedHandle(writeOperator.nodes[i]).setIsWriteLocked(true);
    }

    if (readOperator.checkWriteFailed)
    {
#ifndef NDEBUG
        nodeHandle.debugSynchronizeWriteFailedOperatorCalled = true;
#endif
        ForEachSynchronizeWriteFailedOperator writeFailedOperator(*this);
        nodeHandle.foreachWrite(id, writeFailedOperator);
    }
    else
    {
#ifndef NDEBUG
        nodeHandle.debugSynchronizeWriteFailedOperatorCalled = false;
#endif
    }
}

template <class _NodesHandle> void Parallel<_NodesHandle>::Node::setWriteSignal(NodeId destNodeId, bool flag)
{
    assert(destNodeId < nodesHandleSize);
    writeSignal[destNodeId] = flag;
}

template <class _NodesHandle> void Parallel<_NodesHandle>::Node::flushWritesToPeers()
{
    // Only perform the write side of synchronization: move events from local
    // WriteCache to shared ReadWriteLocked, making them visible to peer nodes.
    // Safe to call from within an event handler (unlike full synchronize which is not reentrant).
    SynchronizeWriteOperator flushOp(*this);

    // Release barrier to make writes visible to readers
    memoryBarrierRelease();

    // Unlock the write locks we just acquired
    for (int i = flushOp.nodesCount; i > -1; --i)
    {
        nodeHandle.getWriterSharedHandle(flushOp.nodes[i]).setIsWriteLocked(true);
    }
}

template <class _NodesHandle>
typename _NodesHandle::Shared &Parallel<_NodesHandle>::Node::getReferenceToWriterShared(NodeId destNodeId)
{
    assert(destNodeId < nodesHandleSize);
    return nodeHandle.getWriterSharedHandle(destNodeId).getReferenceToShared();
}

template <class _NodesHandle> size_t Parallel<_NodesHandle>::Node::getNodeCount() const noexcept
{
    return nodesHandleSize;
}

} // namespace