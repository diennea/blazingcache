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

import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for slow cclients an fetches
 *
 * @author enrico.olivelli
 */
public class FetchPriorityTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client3 = new CacheClient("theClient3", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.setFetchPriority(1);
                client2.setFetchPriority(5);
                client3.setFetchPriority(2);
                client1.start();
                client2.start();
                client3.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                AtomicInteger fetchesServedByClient1 = new AtomicInteger();
                AtomicInteger fetchesServedByClient2 = new AtomicInteger();
                AtomicInteger fetchesServedByClient3 = new AtomicInteger();

                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient1.incrementAndGet();
                        }
                        return true;
                    }
                });

                client2.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient2.incrementAndGet();
                        }
                        return true;
                    }
                });

                client3.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient3.incrementAndGet();
                        }
                        return true;
                    }
                });

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNull(client3.get("foo"));
                client2.put("foo", data, 0);
                client3.put("foo", data, 0);
                assertNull(client1.get("foo"));
                assertNotNull(client2.get("foo"));
                assertNotNull(client3.get("foo"));
                client1.fetch("foo");
                assertEquals(0, fetchesServedByClient1.get());
                assertEquals(1, fetchesServedByClient2.get());
                assertEquals(0, fetchesServedByClient3.get());

                // change priority and reset, client3 now is the best choice for fetches
                client3.setFetchPriority(100);

                fetchesServedByClient1.set(0);
                fetchesServedByClient2.set(0);
                fetchesServedByClient3.set(0);
                client1.disconnect();
                client2.disconnect();
                client3.disconnect();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNull(client3.get("foo"));
                client2.put("foo", data, 0);
                client3.put("foo", data, 0);
                assertNotNull(client1.fetch("foo"));
                assertEquals(1, fetchesServedByClient3.get());
                assertEquals(0, fetchesServedByClient2.get());
                assertEquals(0, fetchesServedByClient1.get());

                // change priority and reset, no client will serve fetches any more
                client3.setFetchPriority(0);
                client2.setFetchPriority(0);

                fetchesServedByClient1.set(0);
                fetchesServedByClient2.set(0);
                fetchesServedByClient3.set(0);
                client1.disconnect();
                client2.disconnect();
                client3.disconnect();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNull(client3.get("foo"));
                client2.put("foo", data, 0);
                client3.put("foo", data, 0);
                assertNull(client1.fetch("foo")); // no one can serve fetches!
                assertEquals(0, fetchesServedByClient3.get());
                assertEquals(0, fetchesServedByClient2.get());
                assertEquals(0, fetchesServedByClient1.get());

            }

        }

    }

    @Test
    public void samePriorityTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client3 = new CacheClient("theClient3", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client4 = new CacheClient("theClient4", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.setFetchPriority(1);
                client2.setFetchPriority(2);
                client3.setFetchPriority(5);
                client4.setFetchPriority(5);

                client1.start();
                client2.start();
                client3.start();
                client4.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));
                assertTrue(client4.waitForConnection(10000));

                AtomicInteger fetchesServedByClient1 = new AtomicInteger();
                AtomicInteger fetchesServedByClient2 = new AtomicInteger();
                AtomicInteger fetchesServedByClient3 = new AtomicInteger();
                AtomicInteger fetchesServedByClient4 = new AtomicInteger();

                client1.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient1.incrementAndGet();
                        }
                        return true;
                    }
                });

                client2.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient2.incrementAndGet();
                        }
                        return true;
                    }
                });

                client3.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient3.incrementAndGet();
                        }
                        return true;
                    }
                });

                client4.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetchesServedByClient4.incrementAndGet();
                        }
                        return true;
                    }
                });

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNull(client3.get("foo"));
                assertNull(client3.get("foo"));
                assertNull(client4.get("foo"));

                client3.put("foo", data, 0);
                client4.put("foo", data, 0);

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNotNull(client3.get("foo"));
                assertNotNull(client4.get("foo"));

                int totalFetches = 10;

                for (int i = 0; i < totalFetches; i++) {
                    client1.fetch("foo");
                    client1.disconnect();
                    assertTrue(client1.waitForConnection(10000));
                }

                assertEquals(totalFetches, fetchesServedByClient3.get() + fetchesServedByClient4.get());
                assertTrue(fetchesServedByClient1.get() == 0);
                assertTrue(fetchesServedByClient2.get() == 0);
                assertTrue(fetchesServedByClient3.get() > 0);
                assertTrue(fetchesServedByClient4.get() > 0);
            }
        }
    }

}
