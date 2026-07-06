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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/**
 * Writer-starvation stress for issue #188.
 * <p>
 * Since the fix, a fetch takes a SHARED (read) lock on the key while an invalidate
 * takes the EXCLUSIVE (write) lock. {@link java.util.concurrent.locks.StampedLock}
 * is not fair, so in principle a continuous stream of readers could starve a writer.
 * This test hammers a single hot key with a large number of concurrent fetchers
 * served with a deliberately long per-fetch latency (long-held read locks), while a
 * single invalidator plays the role of the writer under test, and checks that the
 * invalidator is never starved for longer than {@link #OP_WATCHDOG_MS}.
 * <p>
 * Note that read pressure on a single key is naturally self-limiting: once a client
 * has fetched the key it serves subsequent gets from its local cache and stops
 * hitting the server, so read locks are only taken again after an invalidation
 * evicts the local copies. The invalidator therefore both drives the re-fetch bursts
 * and measures its own latency against them.
 *
 * @author diego.salvi
 */
public class WriterStarvationTest {

    /** Clients that own the key and serve fetch requests. */
    private static final int HOLDERS = 1;
    /** Many concurrent clients hammering the hot key to keep read locks busy. */
    private static final int FETCHERS = 40;
    /**
     * Long per-fetch service time on the owner: read locks are held for a long time
     * to maximise the chance of starving the invalidate write lock.
     */
    private static final long SERVE_DELAY_MS = 300;
    /** The invalidator (writer under test) is considered starved beyond this. */
    private static final long OP_WATCHDOG_MS = 3_000;
    /** How long to keep hammering the key. */
    private static final long TEST_DURATION_MS = 15_000;

    private static final String KEY = "hot-key";

    @Test(timeout = 180_000)
    public void invalidateIsNotStarvedByFetches() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(60_000);
            cacheServer.start();

            List<CacheClient> clients = new ArrayList<>();
            try {
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
                AtomicLong fetchOps = new AtomicLong(0);
                AtomicLong invalidateOps = new AtomicLong(0);
                AtomicLong maxInvalidateLatencyMs = new AtomicLong(0);
                List<String> starved = new CopyOnWriteArrayList<>();
                List<Throwable> errors = new CopyOnWriteArrayList<>();

                List<Thread> workers = new ArrayList<>();

                for (int i = 0; i < FETCHERS; i++) {
                    CacheClient fetcher = newClient("fetcher-" + i, serverHostData);
                    fetcher.setFetchPriority(0);
                    clients.add(fetcher);
                    fetcher.start();
                    workers.add(new Thread(() -> {
                        while (!terminate.get()) {
                            try {
                                EntryHandle h = fetcher.fetch(KEY);
                                if (h != null) {
                                    h.close();
                                }
                                fetchOps.incrementAndGet();
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                return;
                            } catch (Throwable t) {
                                errors.add(t);
                                return;
                            }
                        }
                    }, "fetcher-thread-" + i));
                }

                // the single writer under test: it also drives re-fetch bursts by
                // evicting the local copies of the fetchers.
                CacheClient invalidator = newClient("invalidator", serverHostData);
                invalidator.setFetchPriority(0);
                clients.add(invalidator);
                invalidator.start();
                workers.add(new Thread(() -> {
                    while (!terminate.get()) {
                        long t0 = System.currentTimeMillis();
                        try {
                            invalidator.invalidate(KEY);
                            // keep the owner populated so fetchers keep hitting the slow serve path
                            owner.load(KEY, data, 0);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        } catch (Throwable t) {
                            errors.add(t);
                            return;
                        }
                        long latency = System.currentTimeMillis() - t0;
                        invalidateOps.incrementAndGet();
                        maxInvalidateLatencyMs.accumulateAndGet(latency, Math::max);
                        if (latency > OP_WATCHDOG_MS) {
                            starved.add("invalidate took " + latency + "ms");
                        }
                    }
                }, "invalidator-thread"));

                assertConnected(clients);

                workers.forEach(Thread::start);
                Thread.sleep(TEST_DURATION_MS);
                terminate.set(true);
                for (Thread t : workers) {
                    t.join(30_000);
                }

                System.out.println("WRITER-STARVATION: fetchOps=" + fetchOps.get()
                        + " invalidateOps=" + invalidateOps.get()
                        + " maxInvalidateLatency=" + maxInvalidateLatencyMs.get() + "ms"
                        + " starved(>" + OP_WATCHDOG_MS + "ms)=" + starved.size()
                        + " errors=" + errors.size());
                starved.stream().limit(20).forEach(s -> System.out.println("  STARVED " + s));
                errors.stream().limit(20).forEach(t -> System.out.println("  ERROR " + t));

                assertEquals("unexpected errors: " + errors, 0, errors.size());
                assertEquals("invalidate (writer) starved by concurrent fetches: " + starved, 0, starved.size());
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
