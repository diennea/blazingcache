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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author dennis.mercuriali
 */
public class InvalidateTest {

    @Test
    public void simpleInvalidate() throws Exception {
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

                CountDownLatch invalidateDone_latch = new CountDownLatch(1);

                client2.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onInvalidateResponse(String key, Message response) {
                        if (response.type == Message.TYPE_ACK) {
                            invalidateDone_latch.countDown();
                        }
                    }
                });

                client1.load("entry", data, -1);
                client2.load("entry", data, -1);
                assertArrayEquals(client1.get("entry").getSerializedData(), data);
                assertArrayEquals(client2.get("entry").getSerializedData(), data);

                Thread invalidate = new Thread(() -> {
                    try {
                        client2.invalidate("entry");
                    } catch (InterruptedException exc) {
                    }
                });

                invalidate.start();
                invalidate.join();

                assertEquals(0L, invalidateDone_latch.getCount());

                System.out.println("key_client1:" + cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()));
                System.out.println("key_client2:" + cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()));

                assertTrue(cacheServer.getCacheStatus().getKeysForClient(client1.getClientId()).isEmpty());
                assertTrue(cacheServer.getCacheStatus().getKeysForClient(client2.getClientId()).isEmpty());

                assertNull(client1.get("entry"));
                assertNull(client2.get("entry"));
            }
        }
    }
}
