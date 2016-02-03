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
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for slow cclients an fetches
 *
 * @author enrico.olivelli
 */
public class LostFetchMessageTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                // client1 will "lose" fetch requests for given key
                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            String key = (String) message.parameters.get("key");
                            if (key.equals("lost-fetch")) {
                                return false;
                            }
                        }
                        return true;
                    }
                });

                client1.put("lost-fetch", data, 0);

                CountDownLatch latch_before = new CountDownLatch(1);
                CountDownLatch latch = new CountDownLatch(1);
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch_before.countDown();
                            CacheEntry remoteLoad = client2.fetch("lost-fetch");
                            assertTrue(remoteLoad == null);
                            latch.countDown();
                        } catch (Throwable t) {
                            t.printStackTrace();
                            fail(t + "");
                        }
                    }
                });
                thread.start();

                // wait to enter the wait
                assertTrue(latch_before.await(10, TimeUnit.SECONDS));

                // wait for fetches to be issued on network and locks to be held
                for (int i = 0; i < 100; i++) {
                    Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get("lost-fetch");
                    System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                    Thread.sleep(1000);
                    if (locksCountOnKey != null && locksCountOnKey.intValue() == 1) {
                        break;
                    }
                }
                Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get("lost-fetch");
                assertEquals(Integer.valueOf(1), locksCountOnKey);

                // shut down the bad client, the pending fetch will be canceled
                client1.disconnect();

                // wait to exit the wait
                assertTrue(latch.await(20, TimeUnit.SECONDS));

                System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                assertTrue(cacheServer.getLocksManager().getLockedKeys().isEmpty());

                // clean up test
                thread.join();
            }

        }

    }
}
