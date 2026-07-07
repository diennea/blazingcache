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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.utils.RawString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.junit.Test;

/**
 * Deterministic unit tests for {@link KeyedScheduler}. Operation completions are held
 * and fired manually by the test, so scheduling/coalescing behaviour is fully
 * reproducible on a single thread.
 *
 * @author diego.salvi
 */
public class KeyedSchedulerTest {

    private static final RawString K = RawString.of("k");

    private final List<String> events = new ArrayList<>();
    private final Map<String, Runnable> completions = new ConcurrentHashMap<>();

    private Consumer<Runnable> broadcast(String tag) {
        return onComplete -> {
            events.add("bcast:" + tag);
            completions.put(tag, onComplete);
        };
    }

    private Consumer<Runnable> fetchBody(String tag) {
        return onComplete -> {
            events.add("fetch:" + tag);
            completions.put(tag, onComplete);
        };
    }

    private Runnable registration(String tag) {
        return () -> events.add("reg:" + tag);
    }

    private SimpleCallback<RawString> onFinish(String tag) {
        return (result, error) -> events.add(error == null ? "finish:" + tag : "error:" + tag);
    }

    private void fire(String tag) {
        Runnable r = completions.remove(tag);
        assertNotNull("no pending completion for " + tag, r);
        r.run();
    }

    private void put(KeyedScheduler s, String tag, String lockId) {
        s.submitExclusive(K, KeyedScheduler.Verb.PUT, lockId, registration(tag), broadcast(tag), K, onFinish(tag));
    }

    private void load(KeyedScheduler s, String tag) {
        // load has no broadcast
        s.submitExclusive(K, KeyedScheduler.Verb.LOAD, null, registration(tag), null, K, onFinish(tag));
    }

    private void invalidate(KeyedScheduler s, String tag) {
        s.submitExclusive(K, KeyedScheduler.Verb.INVALIDATE, null, registration(tag), broadcast(tag), K, onFinish(tag));
    }

    private void fetch(KeyedScheduler s, String tag) {
        s.submitFetch(K, null, fetchBody(tag), () -> events.add("invalidLock:" + tag));
    }

    @Test
    public void exclusiveOpsSerializeWhenFirstIsInFlight() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null); // starts immediately, broadcast pending
        put(s, "P1", null); // queued (P0 in flight, not in queue -> no coalescing)
        assertEquals(java.util.Arrays.asList("reg:P0", "bcast:P0"), events);
        assertEquals(Integer.valueOf(2), s.getLockedKeys().get(K)); // 1 active writer + 1 queued

        fire("P0");
        // P1 runs now
        assertEquals(java.util.Arrays.asList("reg:P0", "bcast:P0", "finish:P0", "reg:P1", "bcast:P1"), events);
        fire("P1");
        assertTrue(s.getLockedKeys().isEmpty());
        assertEquals(0, s.getNumberOfLockedKeys());
    }

    @Test
    public void newerPutSuppressesQueuedPutButKeepsBothRegistrations() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null);  // in flight (blocker)
        put(s, "P1", null);  // queued
        put(s, "P2", null);  // folds P1 into itself (P1 removed from the queue)
        // P0 in flight + P2 queued; P1 was folded (coalesced) and is not counted
        assertEquals(Integer.valueOf(2), s.getLockedKeys().get(K));

        fire("P0");
        // P1 runs suppressed (registration kept, no broadcast), then P2 broadcasts once
        assertEquals(java.util.Arrays.asList("reg:P0", "bcast:P0", "finish:P0", "reg:P1", "reg:P2", "bcast:P2"), events);
        fire("P2");
        // P2 completes and notifies both its own and the coalesced P1 caller
        assertTrue(events.contains("finish:P2"));
        assertTrue(events.contains("finish:P1"));
        assertFalse("P1 must not broadcast", events.contains("bcast:P1"));
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void putSuppressesQueuedLoadPreservingLoadRegistration() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null);   // blocker
        load(s, "L1");        // queued, no broadcast
        put(s, "P2", null);   // dominates LOAD -> suppresses L1

        fire("P0");
        assertEquals(java.util.Arrays.asList("reg:P0", "bcast:P0", "finish:P0", "reg:L1", "reg:P2", "bcast:P2"), events);
        fire("P2");
        assertTrue(events.contains("finish:L1"));
        assertTrue(events.contains("finish:P2"));
    }

    @Test
    public void invalidateSuppressesQueuedWritesOfAnySource() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null);   // blocker
        put(s, "P1", null);   // queued
        load(s, "L1");        // queued
        invalidate(s, "I2");  // dominates everything -> suppresses P1 and L1

        fire("P0");
        // P1, L1 run suppressed (registrations kept), then I2 broadcasts once
        assertEquals(java.util.Arrays.asList("reg:P0", "bcast:P0", "finish:P0", "reg:P1", "reg:L1", "reg:I2", "bcast:I2"), events);
        assertFalse(events.contains("bcast:P1"));
        fire("I2");
        assertTrue(events.contains("finish:P1"));
        assertTrue(events.contains("finish:L1"));
        assertTrue(events.contains("finish:I2"));
    }

    @Test
    public void coalescingStopsAtFetchBarrier() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null);   // blocker (in flight)
        put(s, "P1", null);   // queued
        fetch(s, "F");        // queued behind P1 (writer in flight)
        put(s, "P2", null);   // scan hits fetch first -> P1 NOT suppressed

        fire("P0");           // -> P1 runs and broadcasts (not suppressed)
        assertTrue(events.contains("bcast:P1"));
        fire("P1");           // -> fetch F runs
        assertTrue(events.contains("fetch:F"));
        fire("F");            // -> P2 runs
        assertTrue(events.contains("bcast:P2"));
        fire("P2");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void plainInvalidationAttachesToInFlightInvalidation() {
        KeyedScheduler s = new KeyedScheduler();
        invalidate(s, "I0"); // in flight, accepting coalesce
        invalidate(s, "I1"); // attaches to I0, no new broadcast, not queued
        invalidate(s, "I2"); // attaches to I0 as well

        assertEquals(Integer.valueOf(1), s.getLockedKeys().get(K)); // only I0 counts
        assertEquals(java.util.Arrays.asList("reg:I0", "bcast:I0"), events);

        fire("I0"); // notifies I0 + coalesced I1 + I2
        assertTrue(events.contains("finish:I0"));
        assertTrue(events.contains("finish:I1"));
        assertTrue(events.contains("finish:I2"));
        assertFalse(events.contains("bcast:I1"));
        assertFalse(events.contains("bcast:I2"));
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void fetchesRunConcurrentlyAndWriterIsNotStarved() {
        KeyedScheduler s = new KeyedScheduler();
        fetch(s, "F1"); // starts
        fetch(s, "F2"); // starts concurrently
        assertEquals(java.util.Arrays.asList("fetch:F1", "fetch:F2"), events);
        assertEquals(Integer.valueOf(2), s.getLockedKeys().get(K));

        invalidate(s, "W"); // queued: waits for the in-flight readers
        fetch(s, "F3");     // fairness: queued BEHIND the waiting writer
        assertFalse(events.contains("bcast:W"));
        assertFalse(events.contains("fetch:F3"));

        fire("F1");
        assertFalse("writer waits for all readers", events.contains("bcast:W"));
        fire("F2");
        // now readers drained -> writer runs, F3 still waiting behind it
        assertTrue(events.contains("bcast:W"));
        assertFalse(events.contains("fetch:F3"));

        fire("W");
        assertTrue(events.contains("fetch:F3"));
        fire("F3");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void applicationLockBypassAndQueueing() {
        KeyedScheduler s = new KeyedScheduler();
        final String[] token = new String[1];
        s.submitLock(K, "owner", () -> true,
                (result, error) -> {
                    token[0] = result;
                    events.add("lockFinish:" + result);
                });
        assertNotNull(token[0]);
        assertEquals(1, s.getNumberOfLockedKeys());

        // op carrying the valid token bypasses the queue and runs immediately
        put(s, "OWNED", token[0]);
        assertTrue(events.contains("bcast:OWNED"));
        fire("OWNED");

        // malformed token -> invalid
        put(s, "BAD", "not-a-number");
        assertTrue(events.contains("error:BAD"));

        // wrong token -> invalid
        put(s, "WRONG", "999999");
        assertTrue(events.contains("error:WRONG"));

        // no token while locked -> queued, must NOT run yet
        put(s, "WAIT", null);
        assertFalse(events.contains("bcast:WAIT"));

        // unlock releases and drains the queued op
        s.submitUnlock(K, Long.parseLong(token[0]), (result, error) -> events.add("unlockFinish"));
        assertTrue(events.contains("unlockFinish"));
        assertTrue(events.contains("bcast:WAIT"));
        fire("WAIT");
        assertTrue(s.getLockedKeys().isEmpty());
        assertEquals(0, s.getNumberOfLockedKeys());
    }

    @Test
    public void releaseLocksForClientReleasesHeldLockAndResumesDrain() {
        KeyedScheduler s = new KeyedScheduler();
        s.submitLock(K, "owner", () -> true, (result, error) -> {
        });
        put(s, "WAIT", null); // queued behind the lock
        assertFalse(events.contains("bcast:WAIT"));

        s.releaseLocksForClient("owner");
        assertTrue(events.contains("bcast:WAIT"));
        fire("WAIT");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * A lock request still QUEUED when its client disconnects must be cancelled, not
     * later granted to the gone client (leaving the key locked forever).
     */
    @Test
    public void releaseLocksForClientCancelsQueuedLock() {
        KeyedScheduler s = new KeyedScheduler();
        final long[] owner1Token = new long[1];
        s.submitLock(K, "owner1", () -> true,
                (result, error) -> owner1Token[0] = Long.parseLong(result));

        final boolean[] cancelled = {false};
        s.submitLock(K, "owner2", () -> true,
                (result, error) -> {
                    if (error != null) {
                        cancelled[0] = true;
                    } else {
                        events.add("granted2");
                    }
                });

        s.releaseLocksForClient("owner2"); // owner2 disconnects while queued
        assertTrue("queued lock must be cancelled", cancelled[0]);

        // owner1 unlocks: owner2's lock must NOT be granted (it was cancelled)
        s.submitUnlock(K, owner1Token[0], (result, error) -> {
        });
        assertFalse(events.contains("granted2"));
        assertTrue(s.getLockedKeys().isEmpty());
        assertEquals(0, s.getNumberOfLockedKeys());
    }

    /**
     * A token-carrying (lock-bypass) operation in flight must keep the readers-writer
     * barrier: even after the lock is released, a queued exclusive op must wait for the
     * in-flight bypass op to complete, so their CacheStatus effects cannot interleave.
     */
    @Test
    public void bypassOpHoldsBarrierUntilItCompletesAfterUnlock() {
        KeyedScheduler s = new KeyedScheduler();
        final long[] token = new long[1];
        s.submitLock(K, "owner", () -> true, (result, error) -> token[0] = Long.parseLong(result));
        // owner issues a token-carrying fetch (bypass) that stays in flight
        s.submitFetch(K, Long.toString(token[0]), fetchBody("BF"), () -> events.add("invalidLock:BF"));
        assertTrue(events.contains("fetch:BF"));

        invalidate(s, "INV"); // queued behind the held lock
        assertFalse(events.contains("bcast:INV"));

        // owner unlocks while the bypass fetch is still in flight
        s.submitUnlock(K, token[0], (result, error) -> {
        });
        // the invalidate must STILL wait for the in-flight bypass op
        assertFalse(events.contains("bcast:INV"));

        fire("BF"); // bypass fetch completes -> barrier lifts
        assertTrue(events.contains("bcast:INV"));
        fire("INV");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * If a LOCK request races ahead of its client's disconnect cleanup, the owner is
     * already gone at grant time; the lock must be released instead of granted (else the
     * key would be locked forever), and queued work must resume.
     */
    @Test
    public void lockForAlreadyDisconnectedOwnerIsNotGranted() {
        KeyedScheduler s = new KeyedScheduler();
        final String[] outcome = {null};
        // ownerStillConnected returns false: the client disconnected before the grant
        s.submitLock(K, "gone", () -> false,
                (result, error) -> outcome[0] = error != null ? "aborted" : "granted");
        assertEquals("aborted", outcome[0]);
        assertEquals(0, s.getNumberOfLockedKeys());

        // work submitted afterwards must run (the key is not stuck)
        put(s, "AFTER", null);
        assertTrue(events.contains("bcast:AFTER"));
        fire("AFTER");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * Unlocking with token 0 (the "no lock held" sentinel) on an existing-but-unlocked
     * slot must NOT produce a false success.
     */
    @Test
    public void unlockWithSentinelTokenZeroIsRejected() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null); // in-flight op: the slot exists but holds no lock
        final String[] outcome = {null};
        s.submitUnlock(K, 0L, (result, error) -> outcome[0] = error != null ? "error" : "ok");
        assertEquals("error", outcome[0]);
        fire("P0");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * If the grant reply fails to reach the client, the just-acquired lock must be
     * released so the key is not stalled forever (the client never learns it holds it).
     */
    @Test
    public void lockGrantReplyFailureReleasesLock() {
        KeyedScheduler s = new KeyedScheduler();
        s.submitLock(K, "owner", () -> true, (result, error) -> {
            if (error == null) {
                throw new RuntimeException("reply delivery failed");
            }
        });
        assertEquals(0, s.getNumberOfLockedKeys());

        put(s, "AFTER", null);
        assertTrue(events.contains("bcast:AFTER"));
        fire("AFTER");
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * A reply callback may fire more than once (send failure then reply timeout, see
     * NettyChannel.sendMessageWithAsyncReply); a doubly-fired shared completion must
     * not double-decrement the reader count and let a queued writer start while another
     * fetch is still in flight.
     */
    @Test
    public void doubleFiredSharedCompletionIsIdempotent() {
        KeyedScheduler s = new KeyedScheduler();
        fetch(s, "F1");
        fetch(s, "F2");
        assertEquals(Integer.valueOf(2), s.getLockedKeys().get(K));

        Runnable f1 = completions.get("F1");
        f1.run();
        f1.run(); // double-fire must be a no-op
        // F2 is still in flight, so exactly one reader remains
        assertEquals(Integer.valueOf(1), s.getLockedKeys().get(K));

        completions.get("F2").run();
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void doubleFiredExclusiveCompletionIsIdempotent() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P0", null); // in flight
        put(s, "P1", null); // queued

        Runnable p0 = completions.get("P0");
        p0.run();
        p0.run(); // double-fire must be a no-op
        // P1 must have started exactly once
        assertEquals(1, java.util.Collections.frequency(events, "bcast:P1"));

        completions.get("P1").run();
        assertTrue(s.getLockedKeys().isEmpty());
    }

    /**
     * The lock token must already be valid by the time the lock reply fires, so a
     * client that immediately issues a token-carrying op (modelled here by submitting
     * from within the reply callback) is not rejected as holding an invalid lock.
     */
    @Test
    public void tokenIsValidWhenLockReplyFires() {
        KeyedScheduler s = new KeyedScheduler();
        s.submitLock(K, "owner", () -> true, (result, error) -> put(s, "OWNED", result));
        assertTrue(events.contains("bcast:OWNED"));
        assertFalse(events.contains("error:OWNED"));
    }

    @Test
    public void doubleFiredBypassCompletionIsIdempotent() {
        KeyedScheduler s = new KeyedScheduler();
        final String[] token = new String[1];
        s.submitLock(K, "owner", () -> true, (result, error) -> token[0] = result);

        put(s, "OWNED", token[0]); // bypasses the queue, broadcast pending
        Runnable owned = completions.get("OWNED");
        owned.run();
        owned.run(); // double-fire must be a no-op
        assertEquals(1, java.util.Collections.frequency(events, "finish:OWNED"));
    }

    @Test
    public void parseProvidedLockId() {
        assertNull(KeyedScheduler.parseProvidedLockId("not-a-number"));
        assertNull(KeyedScheduler.parseProvidedLockId(""));
        assertEquals(Long.valueOf(42L), KeyedScheduler.parseProvidedLockId("42"));
    }

    /**
     * A malformed (non-numeric) client-provided lock id must be reported as an invalid
     * lock instead of throwing, so the request is not left hanging until timeout
     * (issue #188). The op body must not run.
     */
    @Test
    public void malformedProvidedLockIdIsInvalidOnExclusive() {
        KeyedScheduler s = new KeyedScheduler();
        put(s, "P", "not-a-number");
        assertTrue(events.contains("error:P"));
        assertFalse(events.contains("bcast:P"));
        assertFalse(events.contains("reg:P"));
        assertTrue(s.getLockedKeys().isEmpty());
    }

    @Test
    public void malformedProvidedLockIdIsInvalidOnFetch() {
        KeyedScheduler s = new KeyedScheduler();
        s.submitFetch(K, "not-a-number", fetchBody("F"), () -> events.add("invalidLock:F"));
        assertTrue(events.contains("invalidLock:F"));
        assertFalse(events.contains("fetch:F"));
        assertTrue(s.getLockedKeys().isEmpty());
    }
}
