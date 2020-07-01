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

/**
 * Test for loadEntry
 *
 * @author enrico.olivelli
 */
public class StuckClientMachineTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try ( CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {

            cacheServer.setSlowClientTimeout(10000);
            cacheServer.start();
            AtomicReference<CacheClient> _client2 = new AtomicReference<>();
            try ( CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));  CacheClient client2 = new CacheClient("theClient2", "ciao",
                    new NettyCacheServerLocator(serverHostData)) {
                @Override
                public void messageReceived(Message message) {
                    // swallow every message
                    // client2 will not answer to the "invaliate" message
                    // client1 has to wait until the server declares client2 dead

                    // simulate a network error, only on the server side part
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
                assertNotNull(client2.fetch("foo"));

                client1.invalidate("foo");

                assertNull(client1.get("foo"));
                
                // client2 does not know that the server had problems and it still holds a copy of the value
                assertNotNull(client2.get("foo"));                 

            }

        }

    }

}
