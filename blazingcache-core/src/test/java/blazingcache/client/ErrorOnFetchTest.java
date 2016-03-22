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

import blazingcache.client.impl.InternalClientListener;
import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for slow cclients an fetches
 *
 * @author enrico.olivelli
 */
public class ErrorOnFetchTest {

    @Test
    public void errorFromClient() throws Exception {
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

                // client1 will "lose" fetch requests for given key
                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            channel.sendReplyMessage(message, Message.ERROR(client1.getClientId(), new Exception("mock error")));
                            return false;

                        }
                        return true;
                    }
                });

                client1.put("lost-fetch", data, 0);

                CacheEntry remoteLoad = client2.fetch("lost-fetch");
                assertTrue(remoteLoad == null);

            }

        }

    }
    
    
    @Test
    public void networkError() throws Exception {
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

                // client1 will "lose" fetch requests for given key
                client1.setInternalClientListener(new InternalClientListener() {

                    @Override
                    public boolean messageReceived(Message message, Channel channel) {
                        if (message.type == Message.TYPE_FETCH_ENTRY) {
                            channel.close();
                            return false;
                        }
                        return true;
                    }
                });

                client1.put("lost-fetch", data, 0);

                CacheEntry remoteLoad = client2.fetch("lost-fetch");
                assertTrue(remoteLoad == null);

            }

        }

    }
}
