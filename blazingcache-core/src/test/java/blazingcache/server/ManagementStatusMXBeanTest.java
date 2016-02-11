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
package blazingcache.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import blazingcache.ZKTestEnv;
import blazingcache.client.CacheClient;
import blazingcache.client.CacheClientBuilder;
import blazingcache.client.CacheClientTestUtils;
import blazingcache.client.KeyLock;
import blazingcache.client.CacheClientBuilder.Mode;
import blazingcache.management.JMXUtils;
import blazingcache.network.ServerHostData;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;

/**
 * Test for Server status MX Bean.
 *
 * @author matteo.casadei
 *
 */
public final class ManagementStatusMXBeanTest {

    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 1234;
    private static final String CLIENT_NAME = "testClient";
    private static final String SECRET_TEXT = "mymostsecretsecret";
    private static final String STATUS_MBEAN_PATTERN = "blazingcache.server.management:type=CacheServerStatus,CacheServer={0}";
    private static final int CLIENT_CONNECTION_TIMEOUT = 10000;
    private static final byte[] TEST_DATA = "testdata".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_NO_OF_ENTRIES_BEFORE_EVICTION = 20;

    /**
     * Check that jmx status bean is unavailable by default.
     *
     * @throws Exception
     */
    @Test(expected = InstanceNotFoundException.class)
    public void statusMBeanDisabled() throws Exception {
        try (final CacheServer server1 = new CacheServer(SECRET_TEXT,
                new ServerHostData(SERVER_HOST, SERVER_PORT + 1, "managementStatusTest", false, null));) {
            server1.start();
            final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, server1.getServerId()));
            JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CurrentTimestamp");
        }
    }

    /**
     * Checks that jmx status bean is registered when jmx enabled on
     * ClientCache.
     * <p>
     * Performs also a few tests to make sure status values provided by the
     * mbean are consistent.
     *
     * @throws Exception
     */
    @Test
    public void statusMBeanEnabled() throws Exception {
        final ServerHostData hostData1 = new ServerHostData(SERVER_HOST, SERVER_PORT, "managementStatusTest", false, null);
        final ServerHostData hostData2 = new ServerHostData(SERVER_HOST, SERVER_PORT + 1, "managementStatusTest", false, null);
        try (final ZKTestEnv zkEnv = new ZKTestEnv(folderZk.getRoot().toPath());
                final CacheServer server1 = new CacheServer(SECRET_TEXT, hostData1);
                final CacheServer server2 = new CacheServer(SECRET_TEXT, hostData2);
                final CacheClient client1 = new CacheClient("testClient", SECRET_TEXT,
                        new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));
                final CacheClient client2 = new CacheClient("testClient", SECRET_TEXT,
                        new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));) {
            final ObjectName statusS1BeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, server1.getServerId()));
            final ObjectName statusS2BeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, server2.getServerId()));

            
            server1.enableJmx(true);
            long state1ChangeTS = (Long) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "StateChangeTimestamp");
            assertEquals(0, state1ChangeTS);
            server1.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), hostData1);

            
            server2.enableJmx(true);
            long state2ChangeTS = (Long) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "StateChangeTimestamp");
            assertEquals(0, state2ChangeTS);
            server2.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), hostData2);

            // start server1
            server1.start();
            state1ChangeTS = (Long) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "StateChangeTimestamp");
            assertTrue(state1ChangeTS > 0);

            final long beforeCurrentTs = System.currentTimeMillis();
            long currentTs = (Long) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "CurrentTimestamp");
            assertTrue(currentTs >= beforeCurrentTs && currentTs <= System.currentTimeMillis());

            int numOfClients = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "ConnectedClients");
            assertEquals(0, numOfClients);

            // connect first client
            client1.start();
            assertTrue(client1.waitForConnection(CLIENT_CONNECTION_TIMEOUT));
            numOfClients = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "ConnectedClients");
            assertEquals(1, numOfClients);

            // connect second client
            client2.start();
            assertTrue(client2.waitForConnection(CLIENT_CONNECTION_TIMEOUT));
            numOfClients = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "ConnectedClients");
            assertEquals(2, numOfClients);

            //put a few entries in client1 and client2 in order to assess the global cacheSize
            final Set<String> c1Keys = CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 15, 0);
            final Set<String> c2Keys = CacheClientTestUtils.fillCacheWithTestData(client2, TEST_DATA, 15, 0);
            int globalCacheSize = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "GlobalCacheSize");
            assertEquals(30, globalCacheSize);

            boolean isLeader = (Boolean) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "Leader");
            assertTrue(isLeader);

            // close second client
            client2.close();
            assertTrue(client2.waitForDisconnection(CLIENT_CONNECTION_TIMEOUT));
            numOfClients = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "ConnectedClients");
            assertEquals(1, numOfClients);
            for (int i = 0; i < 100; i++) {
                if (server1.getCacheStatus().getTotalEntryCount() == 15) {
                    break;
                }
                Thread.sleep(1000);
            }
            globalCacheSize = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS1BeanName, "GlobalCacheSize");
            assertEquals(30 - 15, globalCacheSize);

            // start the second server instance to test isLeader false
            server2.start();
            boolean isLeaderServer2 = (Boolean) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "Leader");
            assertFalse(isLeaderServer2);

            // stop first server: server2 becomes the leader
            server1.close();
            for (int i = 0; i < 100; i++) {
                if (server2.isLeader()) {
                    break;
                }
                Thread.sleep(1000);
            }
            state2ChangeTS = (Long) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "StateChangeTimestamp");
            assertTrue(state2ChangeTS > 0);
            isLeaderServer2 = (Boolean) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "Leader");
            assertTrue(isLeaderServer2);
            globalCacheSize = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "GlobalCacheSize");
            assertEquals(0, globalCacheSize);

            //wait for client 1 to reconnect
            for (int i = 0; i < 100; i++) {
                if (server2.getNumberOfConnectedClients() == 1) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertTrue(client1.isConnected());
            Set<String> latestAddedKeys = CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 12, 0);
            globalCacheSize = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "GlobalCacheSize");
            assertEquals(12, globalCacheSize);

            //PENDING OPERATIONS
            long pendingOperations = (Long) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "PendingOperations");
            assertEquals(0, pendingOperations);

            //generate a lot of pending ops: make sure the server has pending ops
            latestAddedKeys.stream().forEach(key -> client1.touchEntry(key, System.currentTimeMillis() + 5000));
            //make sure every pending operation is completed
            for (int i = 0; i < 100; i++) {
                if (client1.getClientTouches() == 12) {
                    break;
                }
                Thread.sleep(1000);
            }
            assertEquals(12, client1.getClientTouches());
            pendingOperations = (Long) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "PendingOperations");
            assertEquals(0, pendingOperations);

            int lockedEntries = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "LockedEntries");
            assertEquals(0, lockedEntries);

            //lock a few fresh entries
            latestAddedKeys = CacheClientTestUtils.fillCacheWithTestData(client1, TEST_DATA, 12, 0);
            final Set<KeyLock> acquiredLocks = new HashSet<>();
            latestAddedKeys.stream().forEach(key -> {
                try {
                    acquiredLocks.add(client1.lock(key));
                } catch (Exception e) {
                    throw new RuntimeException("Not expected to get failed locks here");
                }
            });
            lockedEntries = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "LockedEntries");
            assertEquals(12, lockedEntries);

            //unlock a few
            acquiredLocks.stream().limit(5).forEach(lock -> {
                try {
                    client1.unlock(lock);
                } catch (Exception e) {
                    throw new RuntimeException("Not expected to get failed locks here");
                }
            });
            lockedEntries = (Integer) JMXUtils.getMBeanServer().getAttribute(statusS2BeanName, "LockedEntries");
            assertEquals(12 - 5, lockedEntries);

        } catch (InstanceNotFoundException ex) {
            ex.printStackTrace();
            fail("BlazingCacheServerStatusMXBean is expected to be present as jmx is enabled");
        }
    }
}
