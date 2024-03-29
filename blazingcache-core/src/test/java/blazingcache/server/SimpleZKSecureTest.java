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
package blazingcache.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import blazingcache.ZKTestEnv;
import blazingcache.client.CacheClient;
import blazingcache.network.ServerHostData;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;
import java.io.File;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 * @author enrico.olivelli
 */
public class SimpleZKSecureTest {

    @BeforeClass
    public static void setUpJaas() {
        System.setProperty("java.security.auth.login.config", new File("src/test/resources/test_jaas.conf").getAbsolutePath());
    }

    @AfterClass
    public static void clearUpJaas() {
        System.clearProperty("java.security.auth.login.config");
    }

    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ServerHostData hostData = new ServerHostData("localhost", 1234, "ciao", false, null);
        try (ZKTestEnv zkEnv = new ZKTestEnv(folderZk.getRoot().toPath());
            CacheServer cacheServer = new CacheServer("ciao", hostData)) {
            cacheServer.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), hostData, true);
            cacheServer.start();

            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));
                CacheClient client2 = new CacheClient("theClient2", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                client1.invalidate("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));

            }

        }

    }

    @Test
    public void sessionExpirationTest_SingleCacheServer() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ServerHostData hostData = new ServerHostData("localhost", 1234, "ciao", false, null);
        try (ZKTestEnv zkEnv = new ZKTestEnv(folderZk.getRoot().toPath());
            CacheServer cacheServer = new CacheServer("ciao", hostData)) {
            cacheServer.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), hostData, true);
            cacheServer.start();

            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));
                CacheClient client2 = new CacheClient("theClient2", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                assertEquals(1, client1.getCacheSize());
                assertEquals(1, client2.getCacheSize());

                final long lastStateChangeTS = cacheServer.getStateChangeTimestamp();
                /*
                 * Make's ZooKeeper's session expire:
                 *
                 * this is the session id and password to use on a second zookeeper
                 * handle so as to make service monitor's handle to expire
                 */
                final long serviceZKSessionId = cacheServer.getZooKeeper().getSessionId();
                final byte[] serviceZKpasswd = cacheServer.getZooKeeper().getSessionPasswd();

                CountdownWatcher watch2 = new CountdownWatcher("zkexpire");
                // make session on cache server's cluster manager zk handle expire
                final ZooKeeper zk = new ZooKeeper(zkEnv.getAddress(), zkEnv.getTimeout(), watch2,
                    serviceZKSessionId, serviceZKpasswd);
                watch2.waitForConnected(10000);
                zk.close();
                //first things first, make sure leadership is lost: state change ts has changed
                waitForCondition(() -> {
                    return cacheServer.getStateChangeTimestamp() > lastStateChangeTS;
                }, 100);
                //when fake zk handle expires we are sure that origina cache server session is going to expire
                watch2.waitForExpired(10000);
                //first things first, make sure leadership is acquired again
                waitForCondition(() -> {
                    return cacheServer.isLeader();
                }, 100);

                //ensure clients reconnect
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertEquals(0, client1.getCacheSize());
                assertEquals(0, client2.getCacheSize());
            }
        }
    }

    @Test
    public void sessionExpirationTest_BackupServer() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        final ServerHostData leaderHostdata = new ServerHostData("localhost", 1234, "leader", false, null);
        final ServerHostData backupHostdata = new ServerHostData("localhost", 1235, "backup", false, null);
        try (ZKTestEnv zkEnv = new ZKTestEnv(folderZk.getRoot().toPath());
            CacheServer cacheServer = new CacheServer("ciao", leaderHostdata);
            CacheServer cacheServerBk = new CacheServer("ciao", backupHostdata)) {

            cacheServer.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(),
                zkEnv.getPath(), leaderHostdata, true);
            cacheServer.start();
            waitForCondition(() -> {
                return cacheServer.isLeader();
            }, 100);

            //start backupcluster: we are sure this is in backup mode
            cacheServerBk.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(),
                zkEnv.getPath(), backupHostdata, true);
            cacheServerBk.start();

            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));
                CacheClient client2 = new CacheClient("theClient2", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                assertEquals(1, client1.getCacheSize());
                assertEquals(1, client2.getCacheSize());

                final long lastStateChangeTS = cacheServer.getStateChangeTimestamp();
                /*
                 * Make's ZooKeeper's session expire:
                 *
                 * this is the session id and password to use on a second zookeeper
                 * handle so as to make service monitor's handle to expire
                 */
                final long serviceZKSessionId = cacheServer.getZooKeeper().getSessionId();
                final byte[] serviceZKpasswd = cacheServer.getZooKeeper().getSessionPasswd();

                CountdownWatcher watch2 = new CountdownWatcher("zkexpire");
                // make session on cache server's cluster manager zk handle expire
                final ZooKeeper zk = new ZooKeeper(zkEnv.getAddress(), zkEnv.getTimeout(), watch2,
                    serviceZKSessionId, serviceZKpasswd);
                watch2.waitForConnected(10000);
                zk.close();
                //first things first, make sure leadership is lost: state change ts has changed
                waitForCondition(() -> {
                    return cacheServer.getStateChangeTimestamp() > lastStateChangeTS;
                }, 100);
                //when fake zk handle expires we are sure that original cache server session is going to expire
                watch2.waitForExpired(10000);
                //first things first, make sure leadership is acquired again
                waitForCondition(() -> {
                    return client1.getCacheSize() == 0 && client2.getCacheSize() == 0;
                }, 100);
                waitForCondition(() -> {
                    return !cacheServer.isLeader();
                }, 100);
                waitForCondition(() -> {
                    return cacheServerBk.isLeader();
                }, 100);

                //ensure clients reconnect
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                assertEquals(1, client1.getCacheSize());
                assertEquals(1, client2.getCacheSize());

                client1.invalidate("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));
                assertEquals(0, client1.getCacheSize());
                assertEquals(0, client2.getCacheSize());
            }
        }
    }

    /**
     *
     */
    public static void waitForCondition(Callable<Boolean> condition, int seconds) throws Exception {
        try {
            long _start = System.currentTimeMillis();
            long millis = seconds * 1000;
            while (System.currentTimeMillis() - _start <= millis) {
                if (condition.call()) {
                    return;
                }
                Thread.sleep(100);
            }
        } catch (InterruptedException ee) {
            printStackTrace(ee);
            Assert.fail("test interrupted!");
            return;
        } catch (Exception ee) {
            printStackTrace(ee);
            Assert.fail("error while evalutaing condition:" + ee);
            return;
        }
        Assert.fail("condition not met in time!");
    }

    public static void printStackTrace(Throwable t) {
        t.printStackTrace();
    }

}
