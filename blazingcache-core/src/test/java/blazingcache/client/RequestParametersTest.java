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
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RequestParametersTest {

    @Test
    public void testPutParameters() throws Exception {

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client.start();
                assertTrue(client.waitForConnection(10000));
                assertNull(client.get("foo"));

                client.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onRequestSent(Message message) {
                        assertTrue(message.type == Message.TYPE_PUT_ENTRY);
                        assertTrue(message.parameters.get("data") != null);
                        assertTrue(message.parameters.get("key") != null);
                        assertTrue(message.parameters.get("expiretime") != null);
                    }
                });

                client.put("foo", data, 0);
            }
        }
    }

    @Test
    public void testLoadParameters() throws Exception {
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client.start();
                assertTrue(client.waitForConnection(10000));
                assertNull(client.get("foo"));

                client.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onRequestSent(Message message) {
                        assertTrue(message.type == Message.TYPE_LOAD_ENTRY);
                        assertTrue(message.parameters.get("data") == null);
                        assertTrue(message.parameters.get("key") != null);
                        assertTrue(message.parameters.get("expiretime") != null);
                    }
                });

                client.loadObject("foo", "test-me", 0);
            }
        }

    }

    @Test
    public void testInvalidateParameters() throws Exception {

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client.start();
                assertTrue(client.waitForConnection(10000));
                assertNull(client.get("foo"));

                client.put("foo", data, 0);

                client.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onRequestSent(Message message) {
                        assertTrue(message.type == Message.TYPE_INVALIDATE);
                        assertTrue(message.parameters.get("data") == null);
                        assertTrue(message.parameters.get("key") != null);
                        assertTrue(message.parameters.get("expiretime") == null);
                    }
                });

                client.invalidate("foo");
            }
        }
    }

    @Test
    public void testFetchParameters() throws Exception {

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client.start();
                assertTrue(client.waitForConnection(10000));
                assertNull(client.get("foo"));

                client.put("foo", data, 0);


                client.setInternalClientListener(new InternalClientListener() {
                    @Override
                    public void onRequestSent(Message message) {
                        assertTrue(message.type == Message.TYPE_FETCH_ENTRY);
                        assertTrue(message.parameters.get("data") == null);
                        assertTrue(message.parameters.get("key") != null);
                        assertTrue(message.parameters.get("expiretime") == null);
                    }
                });

                client.fetch("foo");
            }
        }
    }
}
