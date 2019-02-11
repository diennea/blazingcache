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

import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Hammer test, index to excercise concurrency managment of refcounts
 *
 * @author enrico.olivelli
 */
public class HammerTest {

    @Test
    public void basicTest() throws Exception {
        final byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                for (int i = 0; i < 100; i++) {
                    String _key = "key" + i;
                    byte[] expectedData = _key.getBytes(StandardCharsets.UTF_8);
                    client1.load(_key, expectedData, 0);
                }
                assertTrue(client2.waitForConnection(10000));

                List<Future> futures = new ArrayList<>();
                ExecutorService threads = Executors.newFixedThreadPool(4);
                try {
                    for (int i = 0; i < 20; i++) {
                        futures.add(threads.submit(() -> {
                            try {
                                int j = ThreadLocalRandom.current().nextInt(100);
                                String _key = "key" + j;
                                byte[] expectedData = _key.getBytes(StandardCharsets.UTF_8);

                                try (CacheEntry entry = client2.fetch(_key);) {
                                    assertNotNull("key " + _key + " not found?", entry);
                                    assertArrayEquals(entry.getSerializedData(), expectedData);
                                }
                                // rewrite again,from client1
                                client1.put(_key, expectedData, 0);
                            } catch (Exception err) {
                                err.printStackTrace();
                                // fail the future
                                throw new RuntimeException(err);
                            }
                        }));
                    }
                    futures.forEach(a -> {
                        try {
                            a.get();
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    });
                } finally {
                    threads.shutdownNow();
                    threads.awaitTermination(10, TimeUnit.SECONDS);
                }

            }

        }

    }
}
