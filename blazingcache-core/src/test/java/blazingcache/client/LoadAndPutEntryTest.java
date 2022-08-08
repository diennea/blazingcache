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
package blazingcache.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Test for load and put entry interactions
 * 
 * @author diego.salvi
 */
public class LoadAndPutEntryTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data1 = "testdata1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "testdata2".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 =
                    new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(
                            serverHostData));
                    CacheClient client3 = new CacheClient("theClient3", "ciao", new NettyCacheServerLocator(
                            serverHostData));) {

                client1.start();
                client2.start();
                client3.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertNull(client3.get("foo"));

                // foo with data1 only on 1
                client1.load("foo", data1, 0);
                EntryHandle handle;

                // fetch foo with data1 on 2
                handle = client2.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data1, handle.getSerializedData());

                // overwrite globally foo with data2 on 1
                client1.put("foo", data2, 0);

                // read local foo with data2
                handle = client1.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // read local foo with data2
                handle = client2.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // fetch remotely foo with data2
                handle = client3.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // overwrite locally foo with data1 on 1
                client1.load("foo", data1, 0);

                // read local foo with data1
                handle = client1.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data1, handle.getSerializedData());

                // read local foo with data2
                handle = client2.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // read local foo with data2
                handle = client3.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // overwrite globally foo with data2 on 3
                // foo on 1 must change too
                client3.put("foo", data2, 0);

                // read pushed local foo with data2
                handle = client1.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // read pushed local foo with data2
                handle = client2.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

                // read pushed local foo with data2
                handle = client3.fetch("foo");
                assertNotNull(handle);
                assertArrayEquals(data2, handle.getSerializedData());

            }

        }

    }

}
