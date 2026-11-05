/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package blazingcache.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import blazingcache.client.CacheClient;
import blazingcache.client.KeyLock;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.utils.RawString;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * An expired entry whose invalidation is stuck (here behind an application lock,
 * which has no timeout) must not prevent the expiration of the other entries.
 */
public class ExpirerBlockedByLockTest {

    @Test(timeout = 60000)
    public void lockedExpiredEntryDoesNotBlockOtherExpirations() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        RawString k1 = RawString.of("k1");
        RawString k2 = RawString.of("k2");

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setExpirerPeriod(100);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData))) {
                client1.start();
                assertTrue(client1.waitForConnection(10000));

                // k1 expires while locked: its expiration invalidation waits for the unlock
                KeyLock lock = client1.lock("k1");
                assertNotNull(lock);
                client1.put("k1", data, System.currentTimeMillis() + 500, lock);
                waitFor(() -> !cacheServer.getCacheStatus().getKeys().isEmpty(), "k1 not registered");
                Thread.sleep(1500);

                // k2 expires later and must be expired even if k1 is still locked
                client1.load("k2", data, System.currentTimeMillis() + 500);
                waitFor(() -> !cacheServer.getCacheStatus().getKeys().contains(k2), "k2 has not been expired");
                assertTrue(cacheServer.getCacheStatus().getKeys().contains(k1));

                // once unlocked, k1 gets expired too
                client1.unlock(lock);
                waitFor(() -> !cacheServer.getCacheStatus().getKeys().contains(k1), "k1 has not been expired");
            }
        }
    }

    private interface Condition {

        boolean check() throws Exception;
    }

    private static void waitFor(Condition condition, String message) throws Exception {
        long deadline = System.currentTimeMillis() + 20000;
        while (!condition.check()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError(message);
            }
            Thread.sleep(100);
        }
    }
}
