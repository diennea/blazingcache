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
package blazingcache.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import blazingcache.client.CacheClient;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

/**
 *
 * @author dennis.mercuriali
 */
public class MetricsTest {

    @Test
    public void basicTest() throws Exception {
        TestMetricsManager metricsMan1 = new TestMetricsManager();
        TestMetricsManager metricsMan2 = new TestMetricsManager();

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("mysecret", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 = CacheClient.newBuilder()
                    .serverLocator(new NettyCacheServerLocator(serverHostData))
                    .clientId("client1")
                    .sharedSecret("mysecret")
                    .metricsProvider(metricsMan1.getProvider())
                    .build();  CacheClient client2 = CacheClient.newBuilder()
                            .serverLocator(new NettyCacheServerLocator(serverHostData))
                            .clientId("client2")
                            .sharedSecret("mysecret")
                            .metricsProvider(metricsMan2.getProvider())
                            .build();) {

                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.hits").get(), is(0L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.gets").get(), is(1L));

                assertThat(metricsMan2.getGauges().get("blazingcache.client.hits").get(), is(0L));
                assertThat(metricsMan2.getGauges().get("blazingcache.client.gets").get(), is(1L));

                client1.load("foo", data, 0);
                assertNotNull(client2.fetch("foo"));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.hits").get(), is(0L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.gets").get(), is(1L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.loads").get(), is(1L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.memory.actualusage").get(), is((long) data.length));

                assertThat(metricsMan2.getGauges().get("blazingcache.client.hits").get(), is(1L));
                assertThat(metricsMan2.getGauges().get("blazingcache.client.gets").get(), is(1L));
                assertThat(metricsMan2.getGauges().get("blazingcache.client.fetches").get(), is(1L));
                assertThat(metricsMan2.getGauges().get("blazingcache.client.memory.actualusage").get(), is((long) data.length));
                
                client1.load("bar", data, 0);
                assertThat(metricsMan1.getGauges().get("blazingcache.client.loads").get(), is(2L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.loads").get("foo"), is(1L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.loads").get("bar"), is(1L));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.memory.actualusage").get(), is((long) data.length * 2));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.memory.actualusage").get("foo"), is((long) data.length));
                assertThat(metricsMan1.getGauges().get("blazingcache.client.memory.actualusage").get("bar"), is((long) data.length));
            }
        }
    }
}
