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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
public final class ManagementStatusMXBeanTest {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 1234;
    private static final String CLIENT_NAME = "testClient";
    private static final String SECRET_TEXT = "mymostsecretsecret";
    private static final String STATUS_MBEAN_PATTERN = "blazingcache.client.management:type=CacheClientStatus,CacheClient={0}";
    private static final int CLIENT_CONNECTION_TIMEOUT = 10000;
    private static final byte[] TEST_DATA = "testdata".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_NO_OF_ENTRIES_BEFORE_EVICTION = 20;

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
     * Check that jmx status bean is unavailable by default.
     *
     * @throws Exception
     */
    @Test(expected = InstanceNotFoundException.class)
    public void statusMBeanDisabled() throws Exception {
        try (CacheClient client = CacheClientBuilder.newBuilder().clientId(CLIENT_NAME).clientSecret(SECRET_TEXT).mode(Mode.SINGLESERVER)
                .host(SERVER_HOST).port(SERVER_PORT).build();) {

            client.start();
            assertTrue(client.waitForConnection(CLIENT_CONNECTION_TIMEOUT));

            final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, client.getClientId()));
            JMXUtils.getMBeanServer().getAttribute(statusBeanName, "LastConnectionTimestamp");
        }
    }

    /**
     * Check that jmx status bean is registered when jmx enabled on ClientCache.
     *
     * @throws Exception
     */
    @Test
    public void statusMBeanEnabled() throws Exception {
        try (CacheClient client = CacheClientBuilder.newBuilder()
                .clientId("testClient")
                .clientSecret(SECRET_TEXT)
                .mode(Mode.SINGLESERVER)
                .jmx(true)
                .host(SERVER_HOST).port(SERVER_PORT).build();) {

            final long connectionBeforeTs = System.nanoTime();
            client.start();
            assertTrue(client.waitForConnection(CLIENT_CONNECTION_TIMEOUT));
            final long connectionAfterTs = System.nanoTime();

            final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, client.getClientId()));
            try {
                Long connectionTimeStamp = (Long)JMXUtils.getMBeanServer().getAttribute(statusBeanName, "LastConnectionTimestamp");
                assertTrue(connectionTimeStamp > connectionBeforeTs && connectionTimeStamp < connectionAfterTs);

                Long oldestEvictedKeyAge = (Long)JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheOldestEvictedKeyAge");
                assertEquals(0, oldestEvictedKeyAge.longValue());

                boolean isConnected = (Boolean) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "ClientConnected");
                assertTrue(isConnected);

                final long beforeCurrentTs = System.nanoTime();
                long currentTs = (Long)JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CurrentTimestamp");
                assertTrue(currentTs > beforeCurrentTs && currentTs < System.nanoTime());

                int numberOfkeys = (Integer) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheSize");
                assertEquals(0, numberOfkeys);

                long configuredMaxMemory = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheConfiguredMaxMemory");
                assertEquals(0, configuredMaxMemory);

                long usedMemory = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheUsedMemory");
                assertEquals(0, usedMemory);

                //fill cache with test value so as to check that memory usage goes up
                final Set<String> insertedKeys = CacheClientTestUtils.fillCacheWithTestData(client, TEST_DATA, MAX_NO_OF_ENTRIES_BEFORE_EVICTION, 0);
                usedMemory = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheUsedMemory");
                assertEquals(TEST_DATA.length * MAX_NO_OF_ENTRIES_BEFORE_EVICTION, usedMemory);
                numberOfkeys = (Integer) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheSize");
                assertEquals(MAX_NO_OF_ENTRIES_BEFORE_EVICTION, numberOfkeys);

                final List<String> timeSortedInsertedKeys =  insertedKeys.stream().sorted((entry1, entry2) -> {
                    long diff = Long.parseLong(entry1) - Long.parseLong(entry2);
                    if (diff == 0) {
                        return 0;
                    }
                    return diff > 0 ? 1 : -1;
                }).collect(Collectors.toList());

                //trigger eviction
                CacheClientTestUtils.fillCacheWithTestData(client, TEST_DATA, 1, 0).stream().findFirst().get();
                usedMemory = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheUsedMemory");
                assertEquals(TEST_DATA.length * MAX_NO_OF_ENTRIES_BEFORE_EVICTION + TEST_DATA.length, usedMemory);

                //set a limit for local cache max memory: trigger eviction
                client.setMaxMemory(TEST_DATA.length * MAX_NO_OF_ENTRIES_BEFORE_EVICTION);

                //ensuring eviction has been executed
                boolean evictionExecuted = false;
                for(int i=0; i< 100; i++) {
                    if (client.getActualMemory() == TEST_DATA.length * MAX_NO_OF_ENTRIES_BEFORE_EVICTION) {
                        evictionExecuted = true;
                        break;
                    }
                    Thread.sleep(1000);
                }
                assertTrue(evictionExecuted);
                //ensure the evicted key is now null
                assertNull(client.get(timeSortedInsertedKeys.get(0)));

                final long lastEvictedKeyAge = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheOldestEvictedKeyAge");
                assertTrue(lastEvictedKeyAge > 0);

                configuredMaxMemory = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheConfiguredMaxMemory");
                assertEquals(TEST_DATA.length * MAX_NO_OF_ENTRIES_BEFORE_EVICTION, configuredMaxMemory);

                numberOfkeys = (Integer) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CacheSize");
                assertEquals(MAX_NO_OF_ENTRIES_BEFORE_EVICTION, numberOfkeys);

                client.disconnect();
                assertTrue(client.waitForDisconnection(CLIENT_CONNECTION_TIMEOUT));
                isConnected = (Boolean) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "ClientConnected");
                assertFalse(isConnected);
                connectionTimeStamp = (Long) JMXUtils.getMBeanServer().getAttribute(statusBeanName, "LastConnectionTimestamp");
                assertEquals(0, connectionTimeStamp.longValue());

            } catch (InstanceNotFoundException ex) {
                ex.printStackTrace();
                fail("BlazingCacheClientStatusMXBean is expected to be present as jmx is enabled");
            }
        }
    }
}
