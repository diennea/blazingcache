/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package blazingcache.server;

import blazingcache.utils.RawString;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Per-key event-driven serial scheduler that coordinates the cache operations
 * (fetch/put/load/invalidate/lock/unlock) on the {@link CacheServer} without ever
 * blocking a thread on a lock across a network round-trip.
 * <p>
 * Each key owns a {@link KeySlot} holding a FIFO queue drained <b>asynchronously</b>:
 * submitting an operation enqueues it and returns immediately; when the running
 * operation completes (via its completion callback, typically fired by the network
 * layer) the next operation is started. The only lock ever held is the tiny per-slot
 * {@link ReentrantLock}, taken exclusively for O(1) queue/counter bookkeeping and
 * never while performing I/O and never nested with any other lock, so the design is
 * structurally deadlock-free.
 * <p>
 * Scheduling is a non-blocking readers-writer discipline: fetches are
 * {@link Mode#SHARED} (run concurrently), while put/load/invalidate/lock are
 * {@link Mode#EXCLUSIVE}. FIFO ordering with a writer barrier prevents writer
 * starvation: readers that arrive while a writer is already queued are ordered behind
 * it. The drain loop is iterative and non-reentrant, so a run of synchronously
 * completing operations cannot overflow the stack.
 * <p>
 * The scheduler also performs <b>aggressive coalescing</b> of work on the same key.
 * Each write op has two separable effects: (a) a network broadcast to the interested
 * clients and (b) a {@code CacheStatus} registration mutation. Coalescing may drop
 * only the broadcast (a); the registration (b) is always preserved. Under the
 * dominance order {@code INVALIDATE > PUT > LOAD} a newly submitted write folds the
 * contiguous, dominated, not-yet-started predecessors on the tail of the queue into
 * itself (stopping at a fetch or a non-dominated op): their registrations are replayed
 * in order right before the dominant op's single broadcast, and their callers are
 * notified when that broadcast completes. Additionally, plain invalidations arriving
 * while an invalidation of the same key is in flight attach to it (idempotency)
 * instead of queueing another full broadcast.
 *
 * @author diego.salvi
 */
public final class KeyedScheduler {

    private static final Logger LOGGER = Logger.getLogger(KeyedScheduler.class.getName());

    enum Mode {
        SHARED, EXCLUSIVE
    }

    enum Verb {
        FETCH, PUT, LOAD, INVALIDATE, LOCK
    }

    private final ConcurrentHashMap<RawString, KeySlot> slots = new ConcurrentHashMap<>();
    private final AtomicLong tokenGenerator = new AtomicLong();

    /**
     * Parses a client-provided lock id, returning {@code null} (treated as an invalid
     * lock) instead of throwing on a malformed value, so a bad id is reported to the
     * client rather than left hanging until timeout (issue #188).
     */
    static Long parseProvidedLockId(String clientProvidedLockId) {
        try {
            return Long.parseLong(clientProvidedLockId);
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, "invalid clientProvidedLockId {0}", clientProvidedLockId);
            return null;
        }
    }

    // ------------------------------------------------------------------
    // Public API used by CacheServer
    // ------------------------------------------------------------------

    /**
     * Submits a fetch (shared) operation. {@code body} receives a completion callback
     * it must invoke exactly once when the asynchronous fetch has been fully handled.
     * If a client-provided lock id is given and valid, the op bypasses serialization
     * (the owner already holds exclusivity); if it is malformed or does not match the
     * held lock, {@code onInvalidLock} is invoked instead of {@code body}.
     */
    public void submitFetch(RawString key, String providedLockId,
            Consumer<Runnable> body, Runnable onInvalidLock) {
        Op op = new Op(Verb.FETCH, Mode.SHARED);
        op.sharedBody = body;
        submit(key, op, providedLockId, onInvalidLock);
    }

    /**
     * Submits an exclusive write operation (put/load/invalidate).
     *
     * @param registration synchronous CacheStatus mutation applied, in queue order,
     * right before the broadcast (may be {@code null}); always executed even when the
     * broadcast is coalesced away.
     * @param broadcast asynchronous network work; must invoke the supplied callback
     * once completed. {@code null} means there is no broadcast (e.g. load).
     * @param onFinish invoked with {@code (resultKey, null)} on success or
     * {@code (null, error)} on failure, once this op (and every op coalesced into it)
     * is complete.
     */
    public void submitExclusive(RawString key, Verb verb, String providedLockId,
            Runnable registration, Consumer<Runnable> broadcast,
            RawString resultKey, SimpleCallback<RawString> onFinish) {
        Op op = new Op(verb, Mode.EXCLUSIVE);
        op.registration = registration;
        op.broadcast = broadcast;
        op.resultKey = resultKey;
        op.onFinish = onFinish;
        op.coalescible = providedLockId == null
                && (verb == Verb.PUT || verb == Verb.LOAD || verb == Verb.INVALIDATE);
        submit(key, op, providedLockId,
                () -> onFinish.onResult(null, new Exception("invalid clientProvidedLockId " + providedLockId)));
    }

    /**
     * Acquires an application lock on the key. When the lock op reaches the head of the
     * queue a fresh monotonic token is published atomically as the held token; the slot
     * then stays "held" (no further queued op is drained) until {@link #submitUnlock}
     * (or {@link #releaseLocksForClient} on disconnect) releases it. Operations carrying
     * the matching token bypass the queue in the meantime.
     * <p>
     * {@code ownerStillConnected} is checked at grant time: if the owning client has
     * disconnected by then (its LOCK request having raced ahead of the disconnect
     * cleanup), the lock is immediately released instead of being granted to a gone
     * client, closing the acquire-vs-disconnect window.
     */
    public void submitLock(RawString key, String clientId,
            BooleanSupplier ownerStillConnected, SimpleCallback<String> onFinish) {
        long token = tokenGenerator.incrementAndGet();
        Op op = new Op(Verb.LOCK, Mode.EXCLUSIVE);
        op.sourceClientId = clientId;
        op.lockToken = token;
        op.ownerStillConnected = ownerStillConnected;
        op.lockOnFinish = onFinish;
        while (true) {
            KeySlot slot = slots.computeIfAbsent(key, KeySlot::new);
            slot.lock.lock();
            try {
                if (slot.removed) {
                    continue;
                }
                slot.queue.add(op);
            } finally {
                slot.lock.unlock();
            }
            drain(slot);
            return;
        }
    }

    /**
     * Releases an application lock previously acquired with the given token and resumes
     * the drain of the queued operations. Replies an error if no lock with that token is
     * currently held on the key.
     */
    public void submitUnlock(RawString key, long token, SimpleCallback<String> onFinish) {
        KeySlot slot = slots.get(key);
        boolean released = false;
        if (slot != null) {
            slot.lock.lock();
            try {
                // guard against token 0, the "no lock held" sentinel, so unlocking an
                // unlocked key does not produce a false success
                if (token != 0 && slot.heldToken == token) {
                    slot.heldToken = 0;
                    slot.lockOwnerClientId = null;
                    released = true;
                }
            } finally {
                slot.lock.unlock();
            }
        }
        if (released) {
            // The lock is already released; make sure the queued work behind it is
            // resumed (drain) even if the reply throws, otherwise the key would stall
            // until an unrelated later submit.
            try {
                onFinish.onResult(Long.toString(token), null);
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error replying to unlock for key " + key, t);
            } finally {
                drain(slot);
            }
        } else {
            onFinish.onResult(null, new Exception("no lock held for key " + key + " with id " + token));
        }
    }

    /**
     * Releases every application lock owned by a disconnected client and cancels its
     * still-queued lock requests, then resumes the drain of the affected keys. This is
     * the single, scheduler-driven release path used on disconnect: because the lock
     * ownership ({@code heldToken} + {@code lockOwnerClientId}) is published atomically
     * at dequeue, there is no window in which a held or being-acquired lock is invisible
     * here, and queued lock requests that never ran are cancelled rather than later
     * granted to the gone client.
     */
    public void releaseLocksForClient(String clientId) {
        slots.forEach((key, slot) -> {
            List<SimpleCallback<String>> cancelledLocks = null;
            boolean changed = false;
            slot.lock.lock();
            try {
                if (slot.heldToken != 0 && clientId.equals(slot.lockOwnerClientId)) {
                    slot.heldToken = 0;
                    slot.lockOwnerClientId = null;
                    changed = true;
                }
                for (Iterator<Op> it = slot.queue.iterator(); it.hasNext();) {
                    Op queued = it.next();
                    if (queued.verb == Verb.LOCK && clientId.equals(queued.sourceClientId)) {
                        it.remove();
                        if (cancelledLocks == null) {
                            cancelledLocks = new ArrayList<>();
                        }
                        cancelledLocks.add(queued.lockOnFinish);
                        changed = true;
                    }
                }
            } finally {
                slot.lock.unlock();
            }
            if (cancelledLocks != null) {
                for (SimpleCallback<String> cancelled : cancelledLocks) {
                    try {
                        cancelled.onResult(null, new Exception("client " + clientId
                                + " disconnected while waiting for the lock on " + key));
                    } catch (Throwable t) {
                        LOGGER.log(Level.SEVERE, "error cancelling queued lock for key " + key, t);
                    }
                }
            }
            if (changed) {
                drain(slot);
            }
        });
    }

    // ------------------------------------------------------------------
    // Introspection (compatibility with legacy KeyedLockManager / JMX)
    // ------------------------------------------------------------------

    /**
     * Debug view of the currently "locked" keys and the number of outstanding
     * operations referencing each of them (in-flight + queued + a held application
     * lock). Operations coalesced into an in-flight or dominant one are not counted,
     * mirroring the legacy behaviour where only the broadcast owner held the lock.
     */
    public Map<RawString, Integer> getLockedKeys() {
        HashMap<RawString, Integer> result = new HashMap<>();
        slots.forEach((key, slot) -> {
            slot.lock.lock();
            try {
                int count = slot.activeReaders + (slot.activeWriter ? 1 : 0)
                        + slot.queue.size() + (slot.heldToken != 0 ? 1 : 0) + slot.activeBypass;
                if (count > 0) {
                    result.put(key, count);
                }
            } finally {
                slot.lock.unlock();
            }
        });
        return result;
    }

    /**
     * @return the number of keys currently tracked by the scheduler (at rest, the keys
     * holding an application lock).
     */
    public int getNumberOfLockedKeys() {
        return slots.size();
    }

    // ------------------------------------------------------------------
    // Internal machinery
    // ------------------------------------------------------------------

    private void submit(RawString key, Op op, String providedLockId, Runnable onInvalidLock) {
        Long token = null;
        if (providedLockId != null) {
            token = parseProvidedLockId(providedLockId);
            if (token == null) {
                onInvalidLock.run();
                return;
            }
        }
        while (true) {
            KeySlot slot = slots.computeIfAbsent(key, KeySlot::new);
            boolean bypass = false;
            boolean invalid = false;
            boolean enqueued = false;
            slot.lock.lock();
            try {
                if (slot.removed) {
                    continue;
                }
                if (token != null) {
                    if (slot.heldToken != 0 && slot.heldToken == token) {
                        bypass = true;
                        // count the in-flight bypass op so a concurrent unlock/disconnect
                        // does not let queued exclusive ops start before it completes
                        slot.activeBypass++;
                    } else {
                        invalid = true;
                    }
                } else if (op.coalescible && op.verb == Verb.INVALIDATE
                        && slot.inflightInvalidate != null && slot.inflightInvalidate.acceptingCoalesce) {
                    // idempotent invalidation already in flight: attach and let it notify us
                    slot.inflightInvalidate.coalescedWaiters().add(op.onFinish);
                } else {
                    if (op.coalescible) {
                        foldDominatedTail(slot, op);
                    }
                    slot.queue.add(op);
                    enqueued = true;
                }
            } finally {
                slot.lock.unlock();
            }
            if (invalid) {
                onInvalidLock.run();
                maybeRemove(slot);
            } else if (bypass) {
                runBypass(slot, op);
            } else if (enqueued) {
                drain(slot);
            }
            // (attached) -> nothing: our callback will be invoked by the in-flight invalidation
            return;
        }
    }

    /**
     * Folds the contiguous, dominated, not-yet-started predecessors on the tail of the
     * queue into {@code x}: they are removed from the queue, their registrations are
     * collected (in chronological order) to be replayed before {@code x}'s broadcast,
     * and their callers are collected to be notified when {@code x} completes. Scanning
     * stops at the first fetch (shared) or non-dominated op.
     * Must be called while holding {@code slot.lock}.
     */
    private void foldDominatedTail(KeySlot slot, Op x) {
        // Fast path (by far the most common): nothing on the tail can be folded.
        Op tail = slot.queue.peekLast();
        if (tail == null || tail.mode != Mode.EXCLUSIVE || tail.verb == Verb.LOCK
                || !tail.coalescible || !dominates(x.verb, tail.verb)) {
            return;
        }
        ArrayDeque<Op> absorbedNewestFirst = new ArrayDeque<>();
        Iterator<Op> it = slot.queue.descendingIterator();
        while (it.hasNext()) {
            Op t = it.next();
            if (t.mode != Mode.EXCLUSIVE || t.verb == Verb.LOCK || !t.coalescible || !dominates(x.verb, t.verb)) {
                break;
            }
            absorbedNewestFirst.add(t);
        }
        for (int i = 0; i < absorbedNewestFirst.size(); i++) {
            slot.queue.pollLast();
        }
        // replay in chronological order (oldest first)
        Iterator<Op> chronological = absorbedNewestFirst.descendingIterator();
        while (chronological.hasNext()) {
            Op t = chronological.next();
            if (t.foldedRegistrations != null) {
                x.foldedRegistrations().addAll(t.foldedRegistrations);
            }
            if (t.registration != null) {
                x.foldedRegistrations().add(t.registration);
            }
            if (t.coalescedWaiters != null) {
                x.coalescedWaiters().addAll(t.coalescedWaiters);
            }
            x.coalescedWaiters().add(t.onFinish);
        }
    }

    private static boolean dominates(Verb x, Verb t) {
        switch (x) {
            case INVALIDATE:
                return t == Verb.INVALIDATE || t == Verb.PUT || t == Verb.LOAD;
            case PUT:
                return t == Verb.PUT || t == Verb.LOAD;
            default:
                return false;
        }
    }

    /**
     * Iterative, non-reentrant drain: selects and starts the runnable operations of a
     * slot. Only one thread drains a given slot at a time; completions that fire while
     * a drain is in progress (including synchronous ones triggered from within a
     * started body) simply request another pass instead of recursing.
     */
    private void drain(KeySlot slot) {
        slot.lock.lock();
        try {
            if (slot.draining) {
                slot.drainAgain = true;
                return;
            }
            slot.draining = true;
        } finally {
            slot.lock.unlock();
        }
        while (true) {
            List<Op> toStart = new ArrayList<>();
            slot.lock.lock();
            try {
                slot.drainAgain = false;
                selectRunnable(slot, toStart);
            } finally {
                slot.lock.unlock();
            }
            for (Op op : toStart) {
                runStarted(slot, op);
            }
            slot.lock.lock();
            try {
                if (!slot.drainAgain) {
                    slot.draining = false;
                    break;
                }
            } finally {
                slot.lock.unlock();
            }
        }
        maybeRemove(slot);
    }

    /**
     * Selects the next operations to start, updating the slot counters. Must be called
     * while holding {@code slot.lock}.
     */
    private void selectRunnable(KeySlot slot, List<Op> toStart) {
        // Do not start queued work while the key is application-locked (heldToken) or
        // while lock-bypass operations of the current owner are still in flight
        // (activeBypass): the latter keeps the readers-writer barrier sound across a
        // concurrent unlock/disconnect.
        if (slot.heldToken != 0 || slot.activeBypass > 0) {
            return;
        }
        while (true) {
            Op head = slot.queue.peek();
            if (head == null) {
                return;
            }
            if (head.mode == Mode.EXCLUSIVE) {
                if (slot.activeReaders > 0 || slot.activeWriter) {
                    return;
                }
                slot.queue.poll();
                if (head.verb == Verb.LOCK) {
                    // Publish the lock ownership atomically at dequeue: this is the
                    // single point that makes the lock visible, so a concurrent
                    // disconnect can always release it and a token-carrying op cannot
                    // race ahead of the acquisition. No activeWriter is taken, so the
                    // key is not double-counted while runLock records it.
                    slot.heldToken = head.lockToken;
                    slot.lockOwnerClientId = head.sourceClientId;
                } else {
                    slot.activeWriter = true;
                    if (head.verb == Verb.INVALIDATE && head.coalescible) {
                        slot.inflightInvalidate = head;
                        head.acceptingCoalesce = true;
                    }
                }
                toStart.add(head);
                return;
            } else {
                if (slot.activeWriter) {
                    return;
                }
                slot.queue.poll();
                slot.activeReaders++;
                toStart.add(head);
            }
        }
    }

    private void runStarted(KeySlot slot, Op op) {
        if (op.mode == Mode.SHARED) {
            try {
                op.sharedBody.accept(() -> completeShared(slot, op));
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error running fetch for key " + op.resultKey, t);
                completeShared(slot, op);
            }
            return;
        }
        if (op.verb == Verb.LOCK) {
            runLock(slot, op);
        } else {
            runExclusive(slot, op);
        }
    }

    private void runExclusive(KeySlot slot, Op op) {
        try {
            if (op.foldedRegistrations != null) {
                for (Runnable registration : op.foldedRegistrations) {
                    registration.run();
                }
            }
            if (op.registration != null) {
                op.registration.run();
            }
            Consumer<Runnable> broadcast = op.broadcast;
            if (broadcast == null) {
                completeExclusive(slot, op, op.resultKey, null);
            } else {
                broadcast.accept(() -> completeExclusive(slot, op, op.resultKey, null));
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error running " + op.verb + " for key " + op.resultKey + ", releasing the slot", t);
            completeExclusive(slot, op, null, t);
        }
    }

    private void runLock(KeySlot slot, Op op) {
        // heldToken + lockOwnerClientId were published at dequeue (selectRunnable). Only
        // keep the lock if we still hold it AND the owning client is still connected: a
        // concurrent disconnect may already have released it, or the LOCK request may
        // have raced ahead of the disconnect cleanup (which runs after the connection is
        // removed), in which case the owner is already gone.
        // evaluate liveness outside the slot lock (it calls into the acceptor); the tiny
        // window between this check and the grant is backstopped by releaseLocksForClient,
        // which runs after the connection is removed and finds the published heldToken.
        boolean ownerConnected = op.ownerStillConnected == null || op.ownerStillConnected.getAsBoolean();
        boolean granted = false;
        slot.lock.lock();
        try {
            if (slot.heldToken == op.lockToken) {
                if (ownerConnected) {
                    granted = true;
                } else {
                    // release the lock we just published: the owner disconnected
                    slot.heldToken = 0;
                    slot.lockOwnerClientId = null;
                }
            }
        } finally {
            slot.lock.unlock();
        }
        boolean replied = false;
        try {
            if (granted) {
                op.lockOnFinish.onResult(Long.toString(op.lockToken), null);
            } else {
                op.lockOnFinish.onResult(null, new Exception("lock acquisition aborted for key " + slot.key
                        + " (owner disconnected)"));
            }
            replied = true;
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error replying to lockKey for key " + slot.key, t);
        } finally {
            // If the grant reply failed to reach the client, release the just-acquired
            // lock: the client does not know it holds it, so leaving heldToken set would
            // stall the key forever (the client stays connected, so releaseLocksForClient
            // would never fire).
            if (granted && !replied) {
                slot.lock.lock();
                try {
                    if (slot.heldToken == op.lockToken) {
                        slot.heldToken = 0;
                        slot.lockOwnerClientId = null;
                    }
                } finally {
                    slot.lock.unlock();
                }
            }
            drain(slot);
        }
    }

    private void completeShared(KeySlot slot, Op op) {
        // A reply callback may fire more than once (e.g. send failure then reply
        // timeout, see NettyChannel.sendMessageWithAsyncReply); complete each op at
        // most once so the readers-writer accounting is not corrupted.
        if (!op.completed.compareAndSet(false, true)) {
            return;
        }
        try {
            slot.lock.lock();
            try {
                slot.activeReaders--;
            } finally {
                slot.lock.unlock();
            }
        } finally {
            drain(slot);
        }
    }

    private void completeExclusive(KeySlot slot, Op op, RawString key, Throwable error) {
        // Complete at most once: guards both a doubly-fired reply callback and the
        // race between the broadcast completion and the runExclusive catch block.
        if (!op.completed.compareAndSet(false, true)) {
            return;
        }
        List<SimpleCallback<RawString>> waiters;
        slot.lock.lock();
        try {
            slot.activeWriter = false;
            if (slot.inflightInvalidate == op) {
                op.acceptingCoalesce = false;
                slot.inflightInvalidate = null;
            }
            waiters = op.coalescedWaiters;
        } finally {
            slot.lock.unlock();
        }
        // Notify every waiter (isolating a throwing callback) and always drain, so a
        // reply that fails mid-completion cannot strand the coalesced callers or leave
        // the queued work on the key unresumed.
        try {
            RawString result = error == null ? key : null;
            notifyQuietly(op.onFinish, result, error);
            if (waiters != null) {
                for (SimpleCallback<RawString> waiter : waiters) {
                    notifyQuietly(waiter, result, error);
                }
            }
        } finally {
            drain(slot);
        }
    }

    private static void notifyQuietly(SimpleCallback<RawString> callback, RawString result, Throwable error) {
        if (callback == null) {
            return;
        }
        try {
            callback.onResult(result, error);
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error notifying completion callback", t);
        }
    }

    private void runBypass(KeySlot slot, Op op) {
        if (op.mode == Mode.SHARED) {
            try {
                op.sharedBody.accept(() -> {
                    if (op.completed.compareAndSet(false, true)) {
                        bypassRelease(slot);
                    }
                });
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error running fetch (locked) for key " + op.resultKey, t);
                if (op.completed.compareAndSet(false, true)) {
                    bypassRelease(slot);
                }
            }
            return;
        }
        try {
            if (op.registration != null) {
                op.registration.run();
            }
            Consumer<Runnable> broadcast = op.broadcast;
            if (broadcast == null) {
                bypassFinish(slot, op, op.resultKey, null);
            } else {
                broadcast.accept(() -> bypassFinish(slot, op, op.resultKey, null));
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error running " + op.verb + " (locked) for key " + op.resultKey, t);
            bypassFinish(slot, op, null, t);
        }
    }

    /**
     * Completes a lock-bypass write op at most once: like the queued path, the reply
     * callback may fire more than once, so guard onFinish and the slot bookkeeping.
     */
    private void bypassFinish(KeySlot slot, Op op, RawString key, Throwable error) {
        if (!op.completed.compareAndSet(false, true)) {
            return;
        }
        try {
            op.onFinish.onResult(error == null ? key : null, error);
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "error replying to " + op.verb + " (locked) for key " + op.resultKey, t);
        } finally {
            bypassRelease(slot);
        }
    }

    /**
     * Releases a completed lock-bypass op from the in-flight count and resumes the
     * drain, so queued work that was waiting behind the bypass barrier can start once
     * every bypass op has finished.
     */
    private void bypassRelease(KeySlot slot) {
        slot.lock.lock();
        try {
            slot.activeBypass--;
        } finally {
            slot.lock.unlock();
        }
        drain(slot);
    }

    private void maybeRemove(KeySlot slot) {
        slot.lock.lock();
        try {
            if (!slot.draining && slot.queue.isEmpty() && slot.activeReaders == 0 && !slot.activeWriter
                    && slot.heldToken == 0 && slot.activeBypass == 0 && slot.inflightInvalidate == null) {
                slot.removed = true;
                slots.remove(slot.key, slot);
            }
        } finally {
            slot.lock.unlock();
        }
    }

    /**
     * State of a single key. All mutable fields are guarded by {@link #lock}.
     */
    private static final class KeySlot {

        private final RawString key;
        private final ReentrantLock lock = new ReentrantLock();
        private final ArrayDeque<Op> queue = new ArrayDeque<>();
        private int activeReaders;
        private boolean activeWriter;
        private long heldToken;
        private String lockOwnerClientId;
        private int activeBypass;
        private Op inflightInvalidate;
        private boolean draining;
        private boolean drainAgain;
        private boolean removed;

        KeySlot(RawString key) {
            this.key = key;
        }
    }

    /**
     * A single queued/running operation.
     */
    private static final class Op {

        private final Verb verb;
        private final Mode mode;
        // set for LOCK ops: the owning client, so a disconnect can cancel a queued lock
        private String sourceClientId;
        // completed at most once, even if the reply callback fires more than once
        private final AtomicBoolean completed = new AtomicBoolean();

        // shared (fetch)
        private Consumer<Runnable> sharedBody;

        // exclusive write (put/load/invalidate)
        private Runnable registration;
        private Consumer<Runnable> broadcast;
        private RawString resultKey;
        private SimpleCallback<RawString> onFinish;
        private boolean coalescible;
        // lazily created: most write ops never fold a predecessor nor gather waiters
        private List<Runnable> foldedRegistrations;
        private List<SimpleCallback<RawString>> coalescedWaiters;
        private boolean acceptingCoalesce;

        // lock
        private long lockToken;
        private BooleanSupplier ownerStillConnected;
        private SimpleCallback<String> lockOnFinish;

        Op(Verb verb, Mode mode) {
            this.verb = verb;
            this.mode = mode;
        }

        // Lazy accessors, always called under the owning slot's lock.
        List<Runnable> foldedRegistrations() {
            if (foldedRegistrations == null) {
                foldedRegistrations = new ArrayList<>();
            }
            return foldedRegistrations;
        }

        List<SimpleCallback<RawString>> coalescedWaiters() {
            if (coalescedWaiters == null) {
                coalescedWaiters = new ArrayList<>();
            }
            return coalescedWaiters;
        }
    }
}
