/*
 * Copyright 2016 Diennea S.R.L..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package blazingcache.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import blazingcache.client.CacheClientBuilder.Mode;
import blazingcache.client.management.JMXUtils;
import blazingcache.network.ServerHostData;
import blazingcache.server.CacheServer;

/**
 * Test for Client status MX Bean.
 *
 * @author matteo.casadei
 *
 */
public final class ManagementStatisticsMXBeanTest {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 1234;
    private static final String CLIENT_NAME = "testClient";
    private static final String SECRET_TEXT = "mymostsecretsecret";
    private static final String STATISTICS_MBEAN_PATTERN = "blazingcache.client.management:type=CacheClientStatistics,CacheClient={0}";
    private static final int CLIENT_CONNECTION_TIMEOUT = 10000;
    private static final byte[] TEST_DATA = "testdata".getBytes(StandardCharsets.UTF_8);

    private static CacheServer cacheServer;
    private static ServerHostData serverHostData;

    /**
     * @throws Exception
     *
     */
    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        serverHostData = new ServerHostData(SERVER_HOST, SERVER_PORT, "managementStatusTest", false, null);
        cacheServer = new CacheServer(SECRET_TEXT, serverHostData);
        cacheServer.start();
    }

    /**
     *
     */
    @AfterClass
    public static void cleanUpEnvironment() {
        cacheServer.close();
    }

    /**
     * Check that jmx statistics mbean is unavailable by default.
     *
     * @throws Exception
     */
    @Test(expected = InstanceNotFoundException.class)
    public void statusMBeanDisabled() throws Exception {
        try (CacheClient client = CacheClientBuilder.newBuilder().clientId(CLIENT_NAME).clientSecret(SECRET_TEXT).mode(Mode.SINGLESERVER)
                .host(SERVER_HOST).port(SERVER_PORT).build();) {

            client.start();
            assertTrue(client.waitForConnection(CLIENT_CONNECTION_TIMEOUT));

            final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATISTICS_MBEAN_PATTERN, client.getClientId()));
            JMXUtils.getMBeanServer().getAttribute(statusBeanName, "LastConnectionTimestamp");
        }
    }

    /**
     * Checks that jmx statistics bean is registered when jmx enabled on ClientCache.
     * <p>
     * Performs test to make sure provided statistical values are "correct".
     *
     * @throws Exception
     */
    @Test
    public void statusMBeanEnabled() throws Exception {
        try (CacheClient client1 = CacheClientBuilder.newBuilder()
                .clientId("testClient")
                .clientSecret(SECRET_TEXT)
                .mode(Mode.SINGLESERVER)
                .jmx(true)
                .host(SERVER_HOST).port(SERVER_PORT).build();
             CacheClient client2 = CacheClientBuilder.newBuilder()
                 .clientId("testClient")
                 .clientSecret(SECRET_TEXT)
                 .mode(Mode.SINGLESERVER)
                 .jmx(true)
                 .host(SERVER_HOST).port(SERVER_PORT).build();) {

            client1.start();
            assertTrue(client1.waitForConnection(CLIENT_CONNECTION_TIMEOUT));

            final ObjectName statisticsBeanName = new ObjectName(MessageFormat.format(STATISTICS_MBEAN_PATTERN, client1.getClientId()));
            try {
                //PUTS
                long puts = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientPuts");
                assertEquals(0, puts);

                //fill cache with a couple of test values
                final Set<String> client1InsertedKeys = CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 15, 0);
                puts = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientPuts");
                assertEquals(15, puts);

                //GETS
                long gets = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientGets");
                assertEquals(0, gets);

                for (int i = 0; i < 8; i++) {
                    client1.get("non-existing_key");
                }
                gets = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientGets");
                assertEquals(8, gets);

                //HITS: still expected to be 0
                long hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(0, hits);

                //produce a few hits
                client1InsertedKeys.stream().limit(5).forEach(key -> client1.get(key));
                hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(5, hits);

                long fetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientFetches");
                assertEquals(0, fetches);

                //produce further hits via fetch
                client1InsertedKeys.stream().limit(8).forEach(key -> {
                    try {
                        client1.fetch(key);
                    } catch (Exception e) {
                        throw new RuntimeException("Fetch is not expected to fail here.", e);
                    }
                });
                fetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientFetches");
                assertEquals(8, fetches);

                hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(5 + 8, hits);

                //make sure hits do not go up on unsuccessful gets and fetches
                client1.get("another-nonexisting-key");

                long missedGetsToMissedFetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientMissedGetsToMissedFetches");
                assertEquals(0, missedGetsToMissedFetches);
                client1.fetch("another-nonexisting-key");
                missedGetsToMissedFetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientMissedGetsToMissedFetches");
                assertEquals(1, missedGetsToMissedFetches);

                hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(5 + 8, hits);

                //now let's start a further client and test what happens on statistics when remote retrieval is involved
                client2.start();
                assertTrue(client2.waitForConnection(CLIENT_CONNECTION_TIMEOUT));
                final Set<String> client2nsertedKeys = CacheClientTestUtils.fillCacheWithTestData(client2, TEST_DATA, 5, 0);

                //make sure on Client 1 missed gets to successful fetches is still 0
                long missedGetsToSuccessfulFetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientMissedGetsToSuccessfulFetches");
                assertEquals(0, missedGetsToSuccessfulFetches);

                //simulate a few successful remote fetches on client 1 (client2 is hosting the requested keys
                final Set<String> fetchedKeys = new HashSet<>();
                client2nsertedKeys.stream().limit(3).forEach(key -> {
                    try {
                        client1.fetch(key);
                        fetchedKeys.add(key);
                    } catch (Exception e) {
                        throw new RuntimeException("Fetch is not expected to fail here.", e);
                    }
                });

                missedGetsToSuccessfulFetches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientMissedGetsToSuccessfulFetches");
                assertEquals(3, missedGetsToSuccessfulFetches);
                hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(5 + 8 + 3, hits);

                //trying again on client1 but with gets (local cache) should succeed (hits increased)
                fetchedKeys.stream().limit(3).forEach(key -> {
                    client1.get(key);
                });
                hits = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientHits");
                assertEquals(5 + 8 + 3 + 3, hits);

                //a few other puts (these should involve remote ack by client2 also
                CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 2, 0);
                puts = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientPuts");
                assertEquals(15 + 2, puts);

                //TOUCHES
                long touches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientTouches");
                assertEquals(0, touches);

                //touches a few entries in client1's local cache
                client1InsertedKeys.stream().limit(6).forEach(key -> {
                    client1.touchEntry(key, System.currentTimeMillis() + 2000);
                });
                //ensure 6 touches have been ack by server
                for (int i = 0; i < 100; i++) {
                    if (client1.getClientTouches() == 6) {
                        break;
                    }
                    Thread.sleep(1000);
                }

                touches = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientTouches");
                assertEquals(6, touches);

                //INVALIDATION
                long invalidations = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientInvalidations");
                assertEquals(0, invalidations);
                //insert a few entries to be invalidated later
                final Set<String> keysToInvalidate = CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 4, 0);
                //invalidate the entries
                keysToInvalidate.stream().forEach(key -> {
                    try {
                        client1.invalidate(key);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalidation is not expected to fail here.", e);
                    }
                });
                invalidations = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientInvalidations");
                assertEquals(4, invalidations);

                //invalidation by prefix
                //1. generate 5 keys with the same prefix
                //2. put them on cache
                //3. invalidate them
                Stream.generate(() -> "prefix-" + System.nanoTime())
                    .limit(5)
                    .peek(key -> { try {
                        client1.put(key, TEST_DATA, 0);
                    } catch (Exception e) {
                        throw new RuntimeException("Put is not expected to fail here.", e);
                    }})
                    .forEach(key -> { try {
                        client1.invalidate(key);
                    } catch (Exception e) {
                        throw new RuntimeException("Invalidation is not expected to fail here.", e);
                    }});
                invalidations = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientInvalidations");
                assertEquals(4 + 5, invalidations);

                //EVICTIONS
                //set max memory in order to trigger eviction and read corresponding stats
                //consider that any entry written so far has the same size
                final long expectedMemorySize = client1.getCacheSize() * TEST_DATA.length;
                assertEquals(expectedMemorySize, client1.getActualMemory());

                long evictions = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientEvictions");
                assertEquals(0, evictions);
                //set max memory so as to trigger eviction
                client1.setMaxMemory(expectedMemorySize);
                //add a few more entries so that eviction will be executed
                CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 7, 0);
                //ensuring eviction has been executed
                boolean evictionCompleted = false;
                for (int i = 0; i < 100; i++) {
                    if (client1.getActualMemory() == expectedMemorySize) {
                        evictionCompleted = true;
                        break;
                    }
                    Thread.sleep(1000);
                }
                assertTrue(evictionCompleted);
                evictions = (Long) JMXUtils.getMBeanServer().getAttribute(statisticsBeanName, "ClientEvictions");
                assertEquals(7, evictions);

            } catch (InstanceNotFoundException ex) {
                fail("BlazingCacheClientStatusMXBean is expected to be present as jmx is enabled");
            }
        }
    }
}
