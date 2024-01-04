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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author dennis.mercuriali
 */
public class LoadConcurrencyTest {

    @Test
    public void loadInvalidateConflict() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(120000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                CountDownLatch loadDoneForServer_latch = new CountDownLatch(1);
                CountDownLatch invalidateFinished_latch = new CountDownLatch(1);
                AtomicInteger invalidateCount = new AtomicInteger(0);

                client1.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onLoadResponse(String key, Message response) {
                        if (response.type == Message.TYPE_ACK) {
                            loadDoneForServer_latch.countDown();
                            try {
                                assertTrue(invalidateFinished_latch.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException exc) {
                                throw new RuntimeException(exc);
                            }
                        }
                    }

                    @Override
                    public void onRequestSent(Message message) {
                        if (message.type == Message.TYPE_INVALIDATE) {
                            invalidateCount.incrementAndGet();
                        }
                    }
                });

                client2.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public void onRequestSent(Message message) {
                        if (message.type == Message.TYPE_INVALIDATE) {
                            try {
                                assertTrue(loadDoneForServer_latch.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException exc) {
                                throw new RuntimeException(exc);
                            }
                        }
                    }

                    @Override
                    public void onInvalidateResponse(String key, Message response) {
                        if (response.type == Message.TYPE_ACK) {
                            invalidateFinished_latch.countDown();
                        }
                    }
                });

                client2.load("entry", "test".getBytes(StandardCharsets.UTF_8), -1);

                Thread load = new Thread(() -> {
                    try {
                        client1.load("entry", "test".getBytes(StandardCharsets.UTF_8), -1);
                    } catch (InterruptedException | CacheException err) {
                    }
                });

                Thread invalidation = new Thread(() -> {
                    try {
                        client2.invalidate("entry");
                    } catch (InterruptedException err) {
                    }
                });

                load.start();
                invalidation.start();

                invalidation.join();
                load.join();

                assertEquals(0L, loadDoneForServer_latch.getCount());
                assertEquals(0L, invalidateFinished_latch.getCount());

                System.out.println("key_client1:" + cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()));
                System.out.println("key_client2:" + cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()));

                assertNull(client1.get("entry"));
                assertNull(client2.get("entry"));
                Assert.assertEquals(0, invalidateCount.get());
            }
        }

    }

    @Test
    public void loadPutConflict() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(120000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                CountDownLatch loadDoneForServer_latch = new CountDownLatch(1);
                CountDownLatch putFinished_latch = new CountDownLatch(1);
                AtomicInteger invalidateCount = new AtomicInteger(0);

                client1.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onLoadResponse(String key, Message response) {
                        if (response.type == Message.TYPE_ACK) {
                            loadDoneForServer_latch.countDown();
                            try {
                                assertTrue(putFinished_latch.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException exc) {
                                throw new RuntimeException(exc);
                            }
                        }
                    }

                    @Override
                    public void onRequestSent(Message message) {
                        if (message.type == Message.TYPE_INVALIDATE) {
                            invalidateCount.incrementAndGet();
                        }
                    }
                });

                client2.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public void onRequestSent(Message message) {
                        if (message.type == Message.TYPE_PUT_ENTRY) {
                            try {
                                assertTrue(loadDoneForServer_latch.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException exc) {
                                throw new RuntimeException(exc);
                            }
                        }
                    }

                    @Override
                    public void onPutResponse(String key, Message response) {
                        if (response.type == Message.TYPE_ACK) {
                            putFinished_latch.countDown();
                        }
                    }
                });

                client2.load("entry", "test".getBytes(StandardCharsets.UTF_8), -1);

                Thread load = new Thread(() -> {
                    try {
                        client1.load("entry", "test".getBytes(StandardCharsets.UTF_8), -1);
                    } catch (InterruptedException | CacheException err) {
                    }
                });

                Thread put = new Thread(() -> {
                    try {
                        client2.put("entry", "test2".getBytes(StandardCharsets.UTF_8), -1);
                    } catch (InterruptedException | CacheException err) {
                    }
                });

                load.start();
                put.start();

                put.join();
                load.join();

                assertEquals(0L, loadDoneForServer_latch.getCount());
                assertEquals(0L, putFinished_latch.getCount());

                System.out.println("key_client1:" + cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()));
                System.out.println("key_client2:" + cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()));

                assertNull(client1.get("entry"));
                assertNull(client2.get("entry"));
                Assert.assertEquals(1, invalidateCount.get());
            }
        }
    }
}
