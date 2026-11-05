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
import blazingcache.client.EntryHandle;
import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 * Regression test for https://github.com/diennea/blazingcache/issues/207.
 * <p>
 * A client that never acks invalidations used to stall the expirer thread
 * (waiting for the TTL invalidations to complete). Since the very same thread
 * was the only one checking broadcast reply deadlines, the slow client was never
 * disconnected and the server got stuck forever.
 */
public class ExpirerBlockedBySlowClientTest {

    @Test(timeout = 60000)
    public void slowClientIsDisconnectedEvenIfExpirerIsWaiting() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setSlowClientTimeout(2000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                 CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData)) {
                @Override
                public void messageReceived(Message message) {
                    if (message.type == Message.TYPE_INVALIDATE) {
                        // unresponsive client: never ack invalidations
                        return;
                    }
                    super.messageReceived(message);
                }
            }) {
                client1.start();
                client2.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.load("foo", data, System.currentTimeMillis() + 1000);
                EntryHandle fetched = client2.fetch("foo");
                assertNotNull(fetched);
                fetched.close();

                CacheServerSideConnection client2Connection =
                        cacheServer.getAcceptor().getClientConnections().get(client2.getClientId());
                assertNotNull(client2Connection);
                Channel client2Channel = client2Connection.getChannel();
                assertTrue(client2Channel.isValid());

                // the entry expires, the expirer broadcasts the invalidation and waits for
                // client2 that never answers: the slow client must nevertheless be detected
                // (slowClientTimeout) and disconnected
                long deadline = System.currentTimeMillis() + 30000;
                while (client2Channel.isValid()) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("slow client has not been disconnected");
                    }
                    Thread.sleep(100);
                }

                // once the slow client is gone the expired entry gets cleaned up
                deadline = System.currentTimeMillis() + 30000;
                while (cacheServer.getCacheStatus().getTotalEntryCount() > 0) {
                    if (System.currentTimeMillis() > deadline) {
                        throw new AssertionError("expired entry has not been removed");
                    }
                    Thread.sleep(100);
                }
            }
        }
    }
}
