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
import blazingcache.utils.RawString;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for slow cclients an fetches
 *
 * @author enrico.olivelli
 */
public class LockOnLostFetchMessageAndSlowClientTest {

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
                client1.start();
                client2.start();
                client3.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                // client1 will "lose" fetch requests for given key
                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            RawString key = RawString.of(message.parameters.get("key"));
                            if (key.toString().equals("lost-fetch")) {
                                return false;
                            }
                        }
                        return true;
                    }
                });

                client1.put("lost-fetch", data, 0);

                CountDownLatch latch_before_2 = new CountDownLatch(1);
                CountDownLatch latch_2 = new CountDownLatch(1);
                Thread thread_2 = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch_before_2.countDown();
                            EntryHandle remoteLoad = client2.fetch("lost-fetch");
                            assertTrue(remoteLoad == null);
                            latch_2.countDown();
                        } catch (Throwable t) {
                            t.printStackTrace();
                            fail(t + "");
                        }
                    }
                });
                thread_2.start();

                // wait to enter the wait
                assertTrue(latch_before_2.await(10, TimeUnit.SECONDS));

                // a new client issues the fetch, it MUST wait on the lock on the 'lost-fetch' key
                CountDownLatch latch_before_3 = new CountDownLatch(1);
                CountDownLatch latch_3 = new CountDownLatch(1);
                Thread thread_3 = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch_before_3.countDown();
                            EntryHandle remoteLoad = client3.fetch("lost-fetch");
                            assertTrue(remoteLoad == null);
                            latch_3.countDown();
                        } catch (Throwable t) {
                            t.printStackTrace();
                            fail(t + "");
                        }
                    }
                });
                thread_3.start();

                // wait to enter the wait
                assertTrue(latch_before_3.await(10, TimeUnit.SECONDS));

                // wait to exit the wait
                assertTrue(latch_2.await(10, TimeUnit.SECONDS));
                assertTrue(latch_3.await(10, TimeUnit.SECONDS));

                assertTrue(cacheServer.getLocksManager().getLockedKeys().isEmpty());

                // clean up test
                thread_2.join();
                thread_3.join();
            }

        }

    }
}
