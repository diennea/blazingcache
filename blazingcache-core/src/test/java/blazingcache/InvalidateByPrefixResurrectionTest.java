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

import static org.junit.Assert.assertTrue;
import blazingcache.client.CacheClient;
import blazingcache.client.EntryHandle;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.utils.RawString;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Reproducer for the coherence bug that motivated the two-phase {@code invalidateByPrefix}
 * rework: a prefix invalidation must not leave a client holding an entry the server no
 * longer knows about (which would then miss future invalidations and serve stale data).
 * <p>
 * Each round a holder client owns a key, another client overwrites it with a putEntry
 * (which the server broadcasts to the holder) while a third client invalidates by prefix
 * concurrently. The invariant checked after every round is the safe direction: if the
 * holder still has the key locally, the server must know it holds it.
 */
public class InvalidateByPrefixResurrectionTest {

    private static final String PREFIX = "k";
    private static final RawString KEY = RawString.of("k1");
    private static final int ROUNDS = 300;

    @Test(timeout = 120000)
    public void prefixInvalidationDoesNotResurrectEntries() throws Exception {
        byte[] data = "v".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "v2".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient holder = new CacheClient("holder", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient putter = new CacheClient("putter", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient invalidator = new CacheClient("invalidator", "ciao", new NettyCacheServerLocator(serverHostData))) {
                holder.start();
                putter.start();
                invalidator.start();
                assertTrue(holder.waitForConnection(10000));
                assertTrue(putter.waitForConnection(10000));
                assertTrue(invalidator.waitForConnection(10000));

                for (int i = 0; i < ROUNDS; i++) {
                    // holder owns the key (server registers "holder")
                    holder.put(KEY.toString(), data, 0);

                    // concurrently: putter overwrites the key (the server broadcasts a
                    // PUT_ENTRY to the holder) while a third client invalidates by prefix
                    Thread put = new Thread(() -> {
                        try {
                            putter.put(KEY.toString(), data2, 0);
                        } catch (Exception ignore) {
                        }
                    });
                    Thread inv = new Thread(() -> {
                        try {
                            invalidator.invalidateByPrefix(PREFIX);
                        } catch (Exception ignore) {
                        }
                    });
                    put.start();
                    inv.start();
                    put.join();
                    inv.join();

                    // Safe-direction invariant: never "client holds it, server does not know".
                    EntryHandle local = holder.get(KEY.toString());
                    if (local != null) {
                        local.close();
                        boolean serverKnows = cacheServer.getCacheStatus()
                                .getKeysForClient(holder.getClientId()).contains(KEY);
                        assertTrue("resurrection at round " + i
                                + ": holder has the entry but the server does not know it does",
                                serverKnows);
                    }
                }
            }
        }
    }
}
