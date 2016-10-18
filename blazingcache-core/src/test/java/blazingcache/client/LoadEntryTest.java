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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * Test for loadEntry
 *
 * @author enrico.olivelli
 */
public class LoadEntryTest {

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

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

                client1.load("foo", data, 0);
                assertNotNull(client2.fetch("foo"));

                client3.invalidate("foo");
                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

            }

        }

    }

    @Test
    public void basicTestWithLoadObject() throws Exception {
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

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

                client1.loadObject("foo", "test-me", 0);
                assertEquals("test-me", client2.fetchObject("foo"));

                client3.invalidate("foo");
                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

            }

        }

    }
}
