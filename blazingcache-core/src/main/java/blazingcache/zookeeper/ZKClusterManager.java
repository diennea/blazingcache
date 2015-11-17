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
import org.apache.zookeeper.data.Stat;

/**
 * Cluster Management
 *
 * @author enrico.olivelli
 */
public class ZKClusterManager implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ZKClusterManager.class.getName());

    private final ZooKeeper zk;
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
            LOGGER.log(Level.SEVERE, "ZK event: " + we);
            switch (we.getState()) {
                case Expired:
                    onSessionExpired();
                    break;
                case SyncConnected:
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

    public ZKClusterManager(String zkAddress, int zkTimeout, String basePath, LeaderShipChangeListener listener, byte[] localhostdata) throws Exception {
        this.zk = new ZooKeeper(zkAddress, zkTimeout, new SystemWatcher());
        this.basePath = basePath;
        this.listener = listener;
        this.localhostdata = localhostdata;
        this.leaderpath = basePath + "/leader";
        this.discoverypath = basePath + "/discoverypath";
        this.connectionTimeout = zkTimeout;
    }

    public void start() throws Exception {
        try {
            if (this.zk.exists(basePath, false) == null) {
                LOGGER.log(Level.SEVERE, "creating base path " + basePath);
                try {
                    this.zk.create(basePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + basePath, anyError);
                }
            }
            if (this.zk.exists(discoverypath, false) == null) {
                LOGGER.log(Level.SEVERE, "creating discoverypath path " + discoverypath);
                try {
                    this.zk.create(discoverypath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException anyError) {
                    throw new Exception("Could not init Zookeeper space at path " + discoverypath, anyError);
                }
            }
            String newPath = zk.create(discoverypath + "/brokers", localhostdata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.log(Level.SEVERE, "my own discoverypath path is " + newPath);

        } catch (KeeperException error) {
            throw new Exception("Could not init Zookeeper space at path " + basePath, error);
        }
    }

    private static enum MasterStates {

        ELECTED,
        NOTELECTED,
        RUNNING
    }
    private MasterStates state = MasterStates.NOTELECTED;

    public MasterStates getState() {
        return state;
    }

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

    private void checkMaster() {
        zk.getData(leaderpath, false, masterCheckBallback, null);
    }

    private final Watcher masterExistsWatcher = new Watcher() {

        @Override
        public void process(WatchedEvent we) {
            if (we.getType() == EventType.NodeDeleted) {
                requestLeadership();
            }
        }
    };
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

    private void masterExists() {
        zk.exists(leaderpath, masterExistsWatcher, masterExistsCallback, null);
    }

    private void takeLeaderShip() {
        listener.leadershipAcquired();
    }

    public byte[] getActualMaster() throws Exception {
        try {
            return zk.getData(leaderpath, false, new Stat());
        } catch (KeeperException.NoNodeException noMaster) {
            return null;
        }
    }

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

    private void onSessionExpired() {
        listener.leadershipLost();
    }

    public void requestLeadership() {
        zk.create(leaderpath, localhostdata, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

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

}
