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

import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import blazingcache.network.Message;
import blazingcache.network.netty.NettyChannel;
import blazingcache.server.CacheServerSideConnection;
import java.util.concurrent.atomic.AtomicReference;

public class ApparentlyStuckClientDueToServerSideErrorTest {

    @Test(timeout = 60000)
    public void test() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try ( CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {

            // keep the "declare the errored client dead" path bounded and fast
            cacheServer.setSlowClientTimeout(2000);
            cacheServer.start();
            AtomicReference<CacheClient> _client2 = new AtomicReference<>();
            try ( CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));  CacheClient client2 = new CacheClient("theClient2", "ciao",
                    new NettyCacheServerLocator(serverHostData)) {
                @Override
                public void messageReceived(Message message) {
                    // simulate a network error on the server side of client2's connection:
                    // client2 never processes/acks the message and its connection is dropped, so
                    // client1's invalidate must not hang - the server has to declare client2 dead.
                    CacheServerSideConnection serverSideConnectionPeer2 = cacheServer.getAcceptor().getClientConnections().get(_client2.get().getClientId());
                    // we are sure that we are using the NettyChannel, not JVMChannel
                    NettyChannel channel = (NettyChannel) serverSideConnectionPeer2.getChannel();
                    channel.exceptionCaught(new Exception("dummy unpredictable error"));
                }

            };) {
                _client2.set(client2);
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.load("foo", data, 0);
                EntryHandle fetched = client2.fetch("foo");
                assertNotNull(fetched);
                fetched.close();

                // the invalidate must COMPLETE even though client2 errors instead of acking:
                // the server declares client2 dead (slow-client timeout / connection error) and
                // unblocks. If this regressed, the call would hang and the @Test timeout would fire.
                client1.invalidate("foo");

                assertNull(client1.get("foo"));

                // The server-side error dropped client2's connection; a disconnected client empties
                // its local cache (it can no longer receive invalidations), so it stops serving the
                // now-stale entry. This is deterministic; asserting the opposite ("client2 still
                // holds the value") used to race the reconnect/emptyCache and made the test flaky.
                assertBecomesAbsent(client2, "foo", 30000);
            }

        }

    }

    private static void assertBecomesAbsent(CacheClient client, String key, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            EntryHandle entry = client.get(key);
            if (entry == null) {
                return;
            }
            entry.close();
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("entry " + key + " was still present after " + timeoutMillis + "ms");
            }
            Thread.sleep(50);
        }
    }

}
