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
package blazingcache;

import java.nio.charset.StandardCharsets;
import blazingcache.client.CacheClient;
import blazingcache.client.EntryHandle;
import blazingcache.client.KeyLock;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 * @author enrico.olivelli
 */
public class LockLostTest {

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

                KeyLock lock = client1.lock("pippo");

                client1.invalidate("pippo", lock);
                client1.touchEntry("pippo", System.currentTimeMillis() + 1000 * 60 * 20, lock);
                client1.fetch("pippo", lock);
                assertTrue(client1.put("pippo", data, 0, lock));

                AtomicBoolean waiting = new AtomicBoolean(false);
                Runnable blocked = new Runnable() {
                    public void run() {
                        try {
                            waiting.set(true);
                            client2.fetch("pippo");
                            waiting.set(false);
                        } catch (Throwable t) {
                            t.printStackTrace();
                            fail(t + "");
                        }
                    }
                };

                Thread t = new Thread(blocked, "other-client");
                try {
                    t.setDaemon(true);
                    t.start();
                    {
                        int count = 0;
                        while (!waiting.get()) {
                            Thread.sleep(10);
                            count++;
                            if (count > 1000) {
                                fail("not blocked?");
                            }
                        }
                    }
                    // client closes, so lock is lost
                    client1.close();
                    {
                        int count = 0;
                        while (waiting.get()) {
                            Thread.sleep(10);
                            count++;
                            if (count > 1000) {
                                fail("still blocked ?");
                            }
                        }
                    }
                } finally {
                    t.interrupt();
                }
            }

        }

    }
}
