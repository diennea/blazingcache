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
package blazingcache.client;

import static org.junit.Assert.assertEquals;
import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.utils.RawString;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Reproducer for issue #188: under a storm of concurrent fetch and invalidate
 * operations on a single hot key, the server keeps a per-key WRITE lock held for
 * the whole duration of the network round-trip (see
 * {@code CacheServer.fetchEntry / invalidateKey} which acquire
 * {@code KeyedLockManager.acquireWriteLockForKey} and release it only in the
 * async reply callback).
 * <p>
 * As a consequence every operation on that key is fully serialized and each one
 * holds the lock for a full RTT. When many clients hit the same key at once the
 * per-key queue cannot drain and individual operations become unresponsive
 * ("timeout") on that key, while the rest of the cache is fine.
 * <p>
 * The client masks internal timeouts ({@code invalidate()} retries forever,
 * {@code fetch()} returns null on {@code TimeoutException}), so we observe the
 * pathology with an EXTERNAL watchdog on per-operation wall-clock latency: an
 * operation that takes longer than {@link #OP_WATCHDOG_MS} is counted as a
 * timeout.
 *
 * @author diego.salvi
 */
public class FetchAndInvalidateStormTest {

    /** Client that owns the key and serves fetch requests. */
    private static final int HOLDERS = 1;
    /**
     * Concurrent clients continuously fetching the hot key. Together with the
     * invalidators this gives ~28 distinct clients against a single server,
     * matching the production topology (a few dozen clients on the same hot key).
     */
    private static final int FETCHERS = 24;
    /** Concurrent clients continuously invalidating the hot key. */
    private static final int INVALIDATORS = 3;
    /** Simulated per-message service time on the owner (models real RTT / load). */
    private static final long SERVE_DELAY_MS = 200;
    /**
     * Pause between two operations of the same worker. Keeps each worker in a
     * realistic request rate and, crucially, prevents fetchers from spinning on
     * local cache hits at millions of ops/s (which would burn CPU and turn the
     * test into a CPU-starvation test instead of a lock-serialization test).
     */
    private static final long OP_PACING_MS = 2;
    /** An operation slower than this is considered "timed out" on the hot key. */
    private static final long OP_WATCHDOG_MS = 3_000;
    /** How long to keep hammering the key. */
    private static final long TEST_DURATION_MS = 15_000;

    private static final String KEY = "hot-key";

    @Test(timeout = 180_000)
    public void hotKeyStormDoesNotStall() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            // let the server wait long enough for the (deliberately slow) owner,
            // so fetches SUCCEED but slowly: we want to measure serialization, not
            // server-side fetch timeouts.
            cacheServer.setClientFetchTimeout(60_000);
            cacheServer.start();

            List<CacheClient> clients = new ArrayList<>();
            try {
                // ---- owner clients: hold the key and serve fetches with a delay ----
                for (int i = 0; i < HOLDERS; i++) {
                    CacheClient holder = newClient("holder-" + i, serverHostData);
                    holder.setFetchPriority(10);
                    holder.setInternalClientListener(new InternalClientListener() {
                        @Override
                        public boolean messageReceived(Message message, Channel channel) {
                            if (message.type == Message.TYPE_FETCH_ENTRY) {
                                sleep(SERVE_DELAY_MS);
                            }
                            return true;
                        }
                    });
                    clients.add(holder);
                    holder.start();
                }
                CacheClient owner = clients.get(0);
                assertConnected(clients);
                owner.load(KEY, data, 0);

                AtomicBoolean terminate = new AtomicBoolean(false);
                AtomicLong maxLatencyMs = new AtomicLong(0);
                AtomicLong totalOps = new AtomicLong(0);
                List<String> timeouts = new CopyOnWriteArrayList<>();
                List<Throwable> errors = new CopyOnWriteArrayList<>();

                List<Thread> workers = new ArrayList<>();

                // ---- fetchers ----
                for (int i = 0; i < FETCHERS; i++) {
                    CacheClient fetcher = newClient("fetcher-" + i, serverHostData);
                    // do not let a fetcher serve others: keep the owner as the source
                    fetcher.setFetchPriority(0);
                    clients.add(fetcher);
                    fetcher.start();
                    workers.add(new Thread(() -> hammer(terminate, timeouts, errors, maxLatencyMs, totalOps, "fetch", () -> {
                        EntryHandle h = fetcher.fetch(KEY);
                        if (h != null) {
                            h.close();
                        }
                    }), "fetcher-thread-" + i));
                }

                // ---- invalidators (each re-loads on the owner to keep the key hot) ----
                for (int i = 0; i < INVALIDATORS; i++) {
                    CacheClient invalidator = newClient("invalidator-" + i, serverHostData);
                    invalidator.setFetchPriority(0);
                    clients.add(invalidator);
                    invalidator.start();
                    workers.add(new Thread(() -> hammer(terminate, timeouts, errors, maxLatencyMs, totalOps, "invalidate", () -> {
                        invalidator.invalidate(KEY);
                        // keep the owner populated so fetches keep hitting the slow serve path
                        owner.load(KEY, data, 0);
                    }), "invalidator-thread-" + i));
                }

                assertConnected(clients);

                long start = System.currentTimeMillis();
                workers.forEach(Thread::start);
                Thread.sleep(TEST_DURATION_MS);
                terminate.set(true);
                for (Thread t : workers) {
                    t.join(30_000);
                }
                long elapsed = System.currentTimeMillis() - start;

                System.out.println("STORM: duration=" + elapsed + "ms ops=" + totalOps.get()
                        + " maxLatency=" + maxLatencyMs.get() + "ms"
                        + " timeouts(>" + OP_WATCHDOG_MS + "ms)=" + timeouts.size()
                        + " errors=" + errors.size());
                timeouts.stream().limit(20).forEach(t -> System.out.println("  TIMEOUT " + t));
                errors.stream().limit(20).forEach(t -> System.out.println("  ERROR " + t));

                assertEquals("unexpected errors during storm: " + errors, 0, errors.size());
                assertEquals("operations stalled (>" + OP_WATCHDOG_MS + "ms) on the hot key: " + timeouts,
                        0, timeouts.size());
            } finally {
                for (CacheClient c : clients) {
                    try {
                        c.close();
                    } catch (Throwable ignore) {
                        // best effort
                    }
                }
            }
        }
    }

    private interface Op {
        void run() throws Exception;
    }

    private static void hammer(AtomicBoolean terminate, List<String> timeouts, List<Throwable> errors,
            AtomicLong maxLatencyMs, AtomicLong totalOps, String label, Op op) {
        while (!terminate.get()) {
            long t0 = System.currentTimeMillis();
            try {
                op.run();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                errors.add(t);
                return;
            }
            long latency = System.currentTimeMillis() - t0;
            totalOps.incrementAndGet();
            maxLatencyMs.accumulateAndGet(latency, Math::max);
            if (latency > OP_WATCHDOG_MS) {
                timeouts.add(label + " took " + latency + "ms");
            }
            if (OP_PACING_MS > 0) {
                sleep(OP_PACING_MS);
            }
        }
    }

    private static CacheClient newClient(String id, ServerHostData serverHostData) {
        return new CacheClient(id, "ciao", new NettyCacheServerLocator(serverHostData));
    }

    private static void assertConnected(List<CacheClient> clients) throws InterruptedException {
        for (CacheClient c : clients) {
            if (!c.waitForConnection(10_000)) {
                throw new IllegalStateException("client " + c.getClientId() + " not connected");
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
