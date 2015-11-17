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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.GenericNettyBrokerLocator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * Locates master broker using ZK
 *
 * @author enrico.olivelli
 */
public class ZKCacheServerLocator extends GenericNettyBrokerLocator {

    private static final Logger LOGGER = Logger.getLogger(ZKCacheServerLocator.class.getName());

    @Override
    public void brokerDisconnected() {
    }

    private Supplier<ZooKeeper> zkSupplier;
    private final String basePath;
    private final boolean ownedZk;
    private final String zkAddress;
    private final int zkSessiontimeout;

    @Override
    public String toString() {
        if (ownedZk) {
            return "ZKCacheServerLocator{basePath=" + basePath + ", zkAddress=" + zkAddress + ", zkSessiontimeout=" + zkSessiontimeout + ",lastKnownServer=" + lastKnownServer + '}';
        } else {
            return "ZKCacheServerLocator{automatic,lastKnownServer=" + lastKnownServer + "}";
        }
    }

    public ZKCacheServerLocator(Supplier<ZooKeeper> zk, String basePath) throws Exception {
        this.zkSupplier = zk;
        this.basePath = basePath;
        this.ownedZk = false;
        this.zkAddress = null;
        this.zkSessiontimeout = 0;
    }

    public ZKCacheServerLocator(String zkAddress, int zkSessiontimeout, String basePath) throws Exception {
        this.ownedZk = true;
        this.zkSupplier = null;
        this.basePath = basePath;
        this.zkAddress = zkAddress;
        this.zkSessiontimeout = zkSessiontimeout;
        LOGGER.info("zkAddress:" + zkAddress + ", zkSessionTimeout:" + zkSessiontimeout + " basePath:" + basePath);
    }

    private ServerHostData lastKnownServer;

    @Override
    protected ServerHostData getServer() {

        String leaderPath = basePath + "/leader";
        try {
            byte[] data;
            if (ownedZk) {
                CountDownLatch connection = new CountDownLatch(1);
                try {
                    LOGGER.severe("creating temp ZK client for discovery");
                    ZooKeeper client = new ZooKeeper(zkAddress, zkSessiontimeout, new Watcher() {

                        @Override
                        public void process(WatchedEvent event) {
                            if (event.getState() == Event.KeeperState.SyncConnected || event.getState() == Event.KeeperState.ConnectedReadOnly) {
                                connection.countDown();
                            }
                            LOGGER.severe("process ZL event " + event.getState() + " " + event.getType() + " " + event.getPath());
                        }
                    });
                    try {
                        connection.await(connectTimeout, TimeUnit.MILLISECONDS);
                        LOGGER.severe("zk client is " + client);
                        // se la connessione non sarà stabilita in tempo o c'è qualche problem troveremo un ConnectionLoss ad esempio                                        
                        data = client.getData(leaderPath, false, null);
                    } finally {
                        client.close();
                    }
                } catch (IOException err) {
                    LOGGER.log(Level.SEVERE, "zookeeper client not available: " + err);
                    return null;
                }

            } else {
                ZooKeeper client = zkSupplier.get();
                if (client == null) {
                    LOGGER.log(Level.SEVERE, "zookeeper client available");
                    return null;
                }
                data = client.getData(leaderPath, false, null);
            }
            lastKnownServer = ServerHostData.parseHostdata(data);
            return lastKnownServer;
        } catch (KeeperException.NoNodeException nobroker) {
            LOGGER.log(Level.SEVERE, "zookeeper client error", nobroker);
            return null;
        } catch (KeeperException | InterruptedException err) {
            LOGGER.log(Level.SEVERE, "zookeeper client error", err);
            return null;
        }
    }

    @Override
    public void close() {
        if (this.ownedZk) {
            try {
                zkSupplier.get().close();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        zkSupplier = null;
    }

}
