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

import static org.junit.Assert.assertFalse;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;
import blazingcache.utils.RawString;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Test concurrent fetch and invalidate operations
 *
 * @author diego.salvi
 */
public class FetchAndInvalidateHammerTest {

    private static final class LongDecoder {

        public static byte[] serialize(long l) {
            byte[] result = new byte[Long.BYTES];
            for (int i = Long.BYTES - 1; i >= 0; i--) {
                result[i] = (byte) (l & 0xFF);
                l >>= Byte.SIZE;
            }
            return result;
        }

        public static long deserialize(byte[] b) {
            long result = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                result <<= Byte.SIZE;
                result |= (b[i] & 0xFF);
            }
            return result;
        }

    }
    
    @Test
    public void basicTest() throws Exception {
                
        int testNCacheInvalidations = 1000;
        int bugIfSameAfterNCacheInvalidations = 300;
        boolean terminateOnBugFound = true;
        AtomicBoolean terminate = new AtomicBoolean(false);
        AtomicBoolean bugFound = new AtomicBoolean(false);
        AtomicBoolean verbose = new AtomicBoolean(false);
                
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (
                 CacheClient client1 = new CacheClient("theLoader", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theInvalidator", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client3 = new CacheClient("theFetcher", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                client3.start();

                // Preload
                assertTrue(client1.waitForConnection(10000));
                client1.load("key", LongDecoder.serialize(System.currentTimeMillis()), 0);
                
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                List<Future> futures = new ArrayList<>();
                ExecutorService threads = Executors.newFixedThreadPool(3);
                
                
                AtomicInteger invalidationCount = new AtomicInteger(0);
                
                try {

                    // Loader
                    futures.add(threads.submit(() -> {
                        while (!terminate.get()) {
                            boolean debug = verbose.get();
                            try {
                                EntryHandle handle = client1.fetch("key");
                                if (handle == null) {
                                    long now = System.currentTimeMillis();
                                    if (debug) {
                                        System.out.println("LOADER: fetch failed, loading value " + now);
                                    }
                                    client1.load("key", LongDecoder.serialize(now), 0);
                                }
                            } catch (Exception ex) {
                                terminate.set(true);
                                throw new RuntimeException(ex);
                            }
                        }
                    }));

                    // Invalidator
                    futures.add(threads.submit(() -> {
                        int invalidation = 0;
                        while (!terminate.get() && invalidation < testNCacheInvalidations) {
                            boolean debug = verbose.get();
                            try {
                                client2.invalidate("key");
                                invalidation = invalidationCount.incrementAndGet();
                                if (debug) {
                                    System.out.println("INVALIDATOR: invalidating key round " + invalidation);
                                }
                            } catch (Exception ex) {
                                terminate.set(true);
                                throw new RuntimeException(ex);
                            }
                        }
                        terminate.set(true);
                    }));

                    // Fetcher
                    futures.add(threads.submit(() -> {
                        byte[] data = new byte[Long.BYTES];
                        long previousValue = 0L;
                        int previousInvalidation = 0;
                        while (!terminate.get()) {
                            boolean debug = verbose.get();
                            try {
                                EntryHandle handle = client3.fetch("key");
                                if (handle != null) {
                                    handle.getSerializedDataStream().read(data);
                                    long current = LongDecoder.deserialize(data);
                                    if (debug) {
                                        System.out.println("FETCHER: fetch success, read value " + current);
                                    }
                                    if (previousValue == current) {
                                        int currentInvalidation = invalidationCount.get();
                                        if (previousInvalidation + bugIfSameAfterNCacheInvalidations < currentInvalidation) {
                                            verbose.set(true);
                                            bugFound.set(true);
                                            System.out.println("FETCHER: Entry out of sync " + current
                                                    + " from " + (currentInvalidation - previousInvalidation) + " invalidations, now " + currentInvalidation );
                                                
                                                System.out.println(
                                                        "FETCHER: getClientsForKey " + cacheServer.getCacheStatus().getClientsForKey(RawString.of("key")));
                                                System.out.println(
                                                        "FETCHER: getKeysForClient " + cacheServer.getCacheStatus().getKeysForClient("theFetcher"));
                                                if (terminateOnBugFound) {
                                                    terminate.set(true);
                                                }
                                        }
                                    } else {
                                        previousValue = current;
                                        previousInvalidation = invalidationCount.get();
                                    }
                                }
                            } catch (Exception ex) {
                                terminate.set(true);
                                throw new RuntimeException(ex);
                            }
                        }
                    }));

                    futures.forEach(a -> {
                        try {
                            a.get();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                    
                    assertFalse("Fetching an invalidated key", bugFound.get());
                    
                } finally {
                    threads.shutdownNow();
                    threads.awaitTermination(10, TimeUnit.SECONDS);
                }

            }
        }
    }
}
