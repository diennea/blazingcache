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

import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.ConnectionRequestInfo;
import blazingcache.network.ServerLocator;
import blazingcache.network.ServerNotAvailableException;
import blazingcache.network.ServerRejectedConnectionException;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Tests around cache client builder
 *
 * @author enrico.olivelli
 */
public class CacheClientBuilderTest {

    private final ServerLocator serverLocator = new ServerLocator() {
        @Override
        public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, ServerNotAvailableException, ServerRejectedConnectionException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void brokerDisconnected() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    };

    @Test
    public void testDefault() {
        try (CacheClient client = CacheClient.newBuilder()
                .serverLocator(serverLocator)
                .build();) {
            assertTrue(client.isOffHeap());
            assertTrue(client.getAllocator() instanceof UnpooledByteBufAllocator);
            assertTrue(client.getClientId().startsWith("localhost"));
        }
    }

    @Test
    public void testDefaultLegacyConstructor() {
        try (CacheClient client = new CacheClient("ff", "bar", serverLocator)) {
            assertTrue(client.isOffHeap());
            assertTrue(client.getAllocator() instanceof UnpooledByteBufAllocator);
            assertTrue(client.getClientId().startsWith("ff"));
        }
    }

    @Test
    public void testOffHeap() {
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .offHeap(false)
                .build();) {
            assertFalse(client.isOffHeap());
        }
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .offHeap(true)
                .build();) {
            assertTrue(client.isOffHeap());
        }
    }

    @Test
    public void testUsePooledByteBuffers() {
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .poolMemoryBuffers(true)
                .build();) {
            assertTrue(client.getAllocator() instanceof PooledByteBufAllocator);
        }
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .poolMemoryBuffers(false)
                .build();) {
            assertTrue(client.getAllocator() instanceof UnpooledByteBufAllocator);
        }
    }

    @Test
    public void testClientId() {
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .clientId("foo")
                .build();) {
            assertTrue(client.getClientId().startsWith("foo"));
        }
    }

    @Test
    public void testSharedSecret() {
        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .sharedSecret("aaa")
                .build();) {
            assertEquals("aaa", client.getSharedSecret());
        }
    }

    @Test
    public void testServerLocator() {

        try (CacheClient client = CacheClient
                .newBuilder()
                .serverLocator(serverLocator)
                .build();) {
            assertSame(serverLocator, client.getBrokerLocator());
        }
    }

}
