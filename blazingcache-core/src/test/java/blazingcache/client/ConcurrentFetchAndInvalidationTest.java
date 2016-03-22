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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.xml.ws.Holder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * Test for slow cclients an fetches
 *
 * @author enrico.olivelli
 */
public class ConcurrentFetchAndInvalidationTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        CopyOnWriteArrayList<String> actions = new CopyOnWriteArrayList<>();
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(120000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client3 = new CacheClient("theClient3", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                client3.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));
                CountDownLatch fetch_started_latch = new CountDownLatch(1);
                CountDownLatch invalidate_finished_latch = new CountDownLatch(1);
                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            fetch_started_latch.countDown();
                            actions.add("FETCH-STARTED");
                        }
                        return true;
                    }
                });

                client2.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onFetchResponse(String key, Message message) {
                        System.out.println("FETCH RESULT ON CLIENT2: " + message);
                        try {
                            invalidate_finished_latch.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException bad) {
                        }
                        System.out.println("FETCH RESULT APPLIED TO CLIENT2 MEMORY");
                        actions.add("FETCH-APPLIED");
                    }

                });

                client1.put("entry", "test".getBytes(StandardCharsets.UTF_8), -1);

                Holder<CacheEntry> result = new Holder<>();

                Thread slow_fetch = new Thread(() -> {
                    try {
                        result.value = client2.fetch("entry");
                        System.out.println("FETCH RETURNED " + result.value);
                        actions.add("FETCH-RETURNED");
                    } catch (InterruptedException err) {
                    }
                });

                Thread invalidation = new Thread(() -> {
                    try {
                        fetch_started_latch.await(5, TimeUnit.SECONDS);
                        System.out.println("INVALIDATE STARTING");
                        actions.add("INVALIDATE-STARTED");
                        client3.invalidate("entry");
                        invalidate_finished_latch.countDown();
                        System.out.println("INVALIDATE RETURNED");
                        actions.add("INVALIDATE-RETURNED");
                    } catch (InterruptedException err) {
                    }
                });

                invalidation.start();
                slow_fetch.start();

                invalidation.join();
                slow_fetch.join();

                System.out.println("key_client1:" + cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()));
                System.out.println("key_client2:" + cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()));

                // THIS OPERATION SURELY HAPPENED AFTER THE INVALIDATION, SO IT MUST RETURN null!!
                CacheEntry now = client2.get("entry");
                System.out.println("NOW: " + now);
                System.out.println("actions:" + actions);
                int actionIndex = 0;

                assertTrue(cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()).isEmpty());
                assertTrue(cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()).isEmpty());
                assertNull(now);

            }

        }

    }
}
