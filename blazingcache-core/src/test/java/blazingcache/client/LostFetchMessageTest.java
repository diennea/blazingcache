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
                    public void onConnection(Channel channel) {
                        System.out.println("CLIENT1 onConnection");
                    }

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

                CountDownLatch latch_before = new CountDownLatch(1);
                CountDownLatch latch = new CountDownLatch(1);
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch_before.countDown();
                            EntryHandle remoteLoad = client2.fetch("lost-fetch");
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
                    Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get(RawString.of("lost-fetch"));
                    System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                    Thread.sleep(1000);
                    if (locksCountOnKey != null && locksCountOnKey == 1) {
                        break;
                    }
                }
                Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get(RawString.of("lost-fetch"));
                assertEquals(Integer.valueOf(1), locksCountOnKey);

                
                // even if we do not shut down the bad client we are able to make progress

                // wait to exit the wait
                assertTrue(latch.await(20, TimeUnit.SECONDS));

                System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                assertTrue(cacheServer.getLocksManager().getLockedKeys().isEmpty());

                // clean up test
                thread.join();

                // now the client1 should autoreconnect
                assertTrue(client1.waitForConnection(10000));
            }

        }

    }
    
    
    @Test
    public void basicTestZoombieClient() throws Exception {
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
                
                client1.put("lost-fetch", data, 0);   
                
                client1.suspendProcessing();

                CountDownLatch latch_before = new CountDownLatch(1);
                CountDownLatch latch = new CountDownLatch(1);
                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            latch_before.countDown();
                            EntryHandle remoteLoad = client2.fetch("lost-fetch");
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
                    Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get(RawString.of("lost-fetch"));
                    System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                    Thread.sleep(1000);
                    if (locksCountOnKey != null && locksCountOnKey == 1) {
                        break;
                    }
                }
                Integer locksCountOnKey = cacheServer.getLocksManager().getLockedKeys().get(RawString.of("lost-fetch"));
                assertEquals(Integer.valueOf(1), locksCountOnKey);
                
                // even if we do not shut down the bad client we are able to make progress

                // wait to exit the wait
                assertTrue(latch.await(20, TimeUnit.SECONDS));

                System.out.println("LockedKeys:" + cacheServer.getLocksManager().getLockedKeys());
                assertTrue(cacheServer.getLocksManager().getLockedKeys().isEmpty());

                // clean up test
                thread.join();

                // now the client1 should autoreconnect
                assertTrue(client1.waitForConnection(10000));
            }

        }

    }
}
