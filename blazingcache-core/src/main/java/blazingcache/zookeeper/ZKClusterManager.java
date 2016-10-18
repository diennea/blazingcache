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
package blazingcache.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * Cluster Management.
 *
 * @author enrico.olivelli
 */
public class ZKClusterManager implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ZKClusterManager.class.getName());

    private ZooKeeper zk;
    private final LeaderShipChangeListener listener;

    public ZooKeeper getZooKeeper() {
        return zk;
    }

    boolean isLeader() {
        return state == MasterStates.ELECTED;
    }

    CountDownLatch firstConnectionLatch = new CountDownLatch(1);

    void waitForConnection() throws InterruptedException {
        firstConnectionLatch.await(this.connectionTimeout, TimeUnit.MILLISECONDS);
    }

    private class SystemWatcher implements Watcher {

        @Override
        public void process(WatchedEvent we) {
            LOGGER.log(Level.SEVERE, "CacheServer ZK event: " + we);
            switch (we.getState()) {
                case Expired:
                    onSessionExpired();
                    break;
                case SyncConnected:
                    LOGGER.log(Level.SEVERE, "ZK Session connected. " + zk + "session id: " + zk.getSessionId() + "; session password: " + arraytohexstring(zk.getSessionPasswd()));
                    firstConnectionLatch.countDown();
                    break;
            }
        }
    }

    private final String basePath;
    private final byte[] localhostdata;
    private final String leaderpath;
    private final String discoverypath;
    private final int connectionTimeout;
    private final String zkAddress;
    private final int zkTimeout;
    private final List<ACL> acls;

    /**
     * Creates a new ZooKeeper-based cluster manager.
     *
     * @param zkAddress
     * @param zkTimeout
     * @param basePath
     * @param listener
     * @param localhostdata
     * @throws Exception
     */
    public ZKClusterManager(String zkAddress, int zkTimeout, String basePath,
        LeaderShipChangeListener listener, byte[] localhostdata, boolean writeacls) throws Exception {
        this.zk = new ZooKeeper(zkAddress, zkTimeout, new SystemWatcher());
        this.zkAddress = zkAddress;
        this.zkTimeout = zkTimeout;
        this.basePath = basePath;
        this.listener = listener;
        this.localhostdata = localhostdata;
        this.leaderpath = basePath + "/leader";
        this.discoverypath = basePath + "/discoverypath";
        this.connectionTimeout = zkTimeout;
        this.acls = writeacls ? ZooDefs.Ids.CREATOR_ALL_ACL : ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    /**
     *
     * @throws Exception in case of issue on starting the cluster manager
     */
    public void start() throws Exception {
        try {
            if (this.zk.exists(basePath, false) == null) {
                LOGGER.log(Level.SEVERE, "creating base path " + basePath);
                try {
                    this.zk.create(basePath, new byte[0], acls, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + basePath + ":" + anyError, anyError);
                }
            }
            if (this.zk.exists(discoverypath, false) == null) {
                LOGGER.log(Level.SEVERE, "creating discoverypath path " + discoverypath);
                try {
                    this.zk.create(discoverypath, new byte[0], acls, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + discoverypath, anyError);
                }
            }
            String newPath = zk.create(discoverypath + "/brokers", localhostdata, acls, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.log(Level.SEVERE, "my own discoverypath path is " + newPath);

        } catch (KeeperException error) {
            throw new Exception("Could not init Zookeeper space at path " + basePath + ":" + error, error);
        }
    }

    /**
     *
     * @author enrico.olivelli
     *
     */
    private enum MasterStates {

        ELECTED,
        NOTELECTED,
        RUNNING
    }

    private MasterStates state = MasterStates.NOTELECTED;

    /**
     *
     * @return leader's state
     */
    public MasterStates getState() {
        return state;
    }

    /**
     *
     */
    AsyncCallback.DataCallback masterCheckBallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object o, byte[] bytes, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NONODE:
                    requestLeadership();
                    break;
            }
        }
    };

    /**
     *
     */
    private void checkMaster() {
        zk.getData(leaderpath, false, masterCheckBallback, null);
    }

    /**
     *
     */
    private final Watcher masterExistsWatcher = new Watcher() {

        @Override
        public void process(final WatchedEvent we) {
            if (we.getType() == EventType.NodeDeleted) {
                requestLeadership();
            }
        }
    };

    /**
     *
     */
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {

        @Override
        public void processResult(int rc, String string, Object o, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if (stat == null) {
                        state = MasterStates.RUNNING;
                        requestLeadership();
                    }
                    break;
                default:
                    checkMaster();
            }
        }
    };

    /**
     *
     */
    private void masterExists() {
        zk.exists(leaderpath, masterExistsWatcher, masterExistsCallback, null);
    }

    /**
     *
     */
    private void takeLeaderShip() {
        listener.leadershipAcquired();
    }

    /**
     *
     * @return leader's data
     * @throws Exception
     */
    public byte[] getActualMaster() throws Exception {
        try {
            return zk.getData(leaderpath, false, new Stat());
        } catch (KeeperException.NoNodeException noMaster) {
            return null;
        }
    }

    /**
     *
     */
    private final AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {

        @Override
        public void processResult(int code, String path, Object o, String name) {
            LOGGER.log(Level.INFO, "masterCreateCallback path:" + path + ", code:" + Code.get(code));
            switch (Code.get(code)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    LOGGER.log(Level.SEVERE, "create success at " + path + ", code:" + Code.get(code) + ", I'm the new LEADER");
                    state = MasterStates.ELECTED;
                    takeLeaderShip();
                    break;
                case NODEEXISTS:
                    LOGGER.log(Level.SEVERE, "create failed at " + path + ", code:" + Code.get(code) + ", a LEADER already exists");
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default:
                    LOGGER.log(Level.SEVERE, "bad ZK state " + KeeperException.create(Code.get(code), path));

            }
        }

    };

    /**
     *
     */
    private void onSessionExpired() {
        listener.leadershipLost();
        handleExpiredSession();
    }

    /**
     * Handle session expiration.
     */
    private void handleExpiredSession() {
        // close expired session first
        this.stopZK();

        LOGGER.log(Level.SEVERE, "ZK session expired. trying to recover session");
        try {
            this.restartZK();
            this.registerZKNodes();
            this.requestLeadership();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Cannot create a new zookeeper client on session recovery");
        }
    }

    /**
     * register back brokers ephemeral nodes after session recovery.
     *
     * @throws Exception in case node creation ends up with issues
     */
    private void registerZKNodes() throws Exception {
        final String completeDiscoveryPath = discoverypath + "/brokers";
        String newPath = zk.create(completeDiscoveryPath, localhostdata, acls, CreateMode.EPHEMERAL_SEQUENTIAL);
        LOGGER.log(Level.SEVERE, "my own discoverypath path is " + newPath);
    }

    /**
     *
     * Actually creates the ZooKeeper session.
     *
     * @throws IOException in case of network issues to connect to ZooKeeper service
     */
    public final void restartZK() throws IOException {
        LOGGER.log(Level.SEVERE, "Restarting ZooKeeper client after session expired");
        this.zk = new ZooKeeper(this.zkAddress, this.zkTimeout, new SystemWatcher());
    }

    /**
     * Utility method used to stop an existing ZooKeeper.
     */
    private void stopZK() {
        try {
            this.zk.close();
        } catch (InterruptedException e) {
            LOGGER.log(Level.SEVERE, "Impossible to stop ZooKeeper on expired session", e);
        }
    }

    /**
     * Let cache server compete for cluster leadership.
     */
    public void requestLeadership() {
        zk.create(leaderpath, localhostdata, acls, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    /**
     *
     */
    @Override
    public void close() {
        listener.leadershipLost();
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException ignore) {
            }
        }
    }

    private String arraytohexstring(byte[] bytes) {
        StringBuilder string = new StringBuilder();
        for (byte b : bytes) {
            String hexString = Integer.toHexString(0x00FF & b);
            string.append(hexString.length() == 1 ? "0" + hexString : hexString);
        }
        return string.toString();
    }

}
