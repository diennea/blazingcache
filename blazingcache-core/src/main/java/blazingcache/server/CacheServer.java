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

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyChannelAcceptor;
import blazingcache.zookeeper.LeaderShipChangeListener;
import blazingcache.zookeeper.ZKClusterManager;

/**
 *
 * @author enrico.olivelli
 */
public class CacheServer implements AutoCloseable {

    private final String sharedSecret;
    private final CacheServerEndpoint acceptor;
    private final CacheStatus cacheStatus = new CacheStatus();
    private volatile boolean leader;
    private volatile boolean stopped;
    private ZKClusterManager clusterManager;
    private Thread expireManager;
    private NettyChannelAcceptor server;
    private final static Logger LOGGER = Logger.getLogger(CacheServer.class.getName());

    public CacheServer(String sharedSecret, ServerHostData serverHostData) {
        this.sharedSecret = sharedSecret;
        this.acceptor = new CacheServerEndpoint(this);
        this.server = new NettyChannelAcceptor(serverHostData.getHost(), serverHostData.getPort(), serverHostData.isSsl());
        this.server.setAcceptor(acceptor);
        this.leader = true;
    }

    public void setupSsl(File certificateFile, String password, File certificateChain) {
        this.server.setSslCertChainFile(certificateChain);
        this.server.setSslCertChainFile(certificateFile);
        this.server.setSslCertPassword(password);
    }

    public CacheStatus getCacheStatus() {
        return cacheStatus;
    }

    private class LeaderShipChangeListenerImpl extends LeaderShipChangeListener {

        @Override
        public void leadershipLost() {
            leader = false;
        }

        @Override
        public void leadershipAcquired() {
            leader = true;
        }

    }

    public void setupCluster(String zkAddress, int zkTimeout, String basePath, ServerHostData localhostdata) throws Exception {
        leader = false;
        clusterManager = new ZKClusterManager(zkAddress, zkTimeout, basePath, new LeaderShipChangeListenerImpl(), ServerHostData.formatHostdata(localhostdata));
        clusterManager.start();
        clusterManager.requestLeadership();
    }

    public void start() throws Exception {
        this.stopped = false;
        this.expireManager = new Thread(new Expirer(), "cache-server-expire-thread");
        this.expireManager.setDaemon(true);
        this.expireManager.start();
        this.server.start();
    }

    private class Expirer implements Runnable {

        @Override
        public void run() {
            while (!stopped) {
                if (isLeader()) {
                    long now = System.currentTimeMillis();
                    List<String> entries = cacheStatus.selectExpiredEntries(now, 1000);
                    if (!entries.isEmpty()) {

                        CountDownLatch latch = new CountDownLatch(entries.size());
                        for (String key : entries) {
                            LOGGER.severe("expiring entry " + key);
                            invalidateKey(key, "expire-timer", new SimpleCallback<String>() {

                                @Override
                                public void onResult(String result, Throwable error) {
                                    LOGGER.severe("expired entry " + key + " " + error);
                                    latch.countDown();
                                }
                            });
                        }
                        try {
                            latch.await();
                        } catch (InterruptedException exit) {
                            break;
                        }
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException exit) {
                    break;
                }
            }
        }
    }

    public CacheServerEndpoint getAcceptor() {
        return acceptor;
    }

    public String getSharedSecret() {
        return sharedSecret;
    }

    public boolean isLeader() {
        return leader;
    }

    @Override
    public void close() {
        stopped = true;
        leader = false;
        if (this.server != null) {
            this.server.close();
        }
        if (clusterManager != null) {
            clusterManager.close();
        }
        try {
            expireManager.interrupt();
            expireManager.join(60000);
        } catch (InterruptedException exit) {
        }
    }

    public void putEntry(String key, byte[] data, long expiretime, String sourceClientId, SimpleCallback<String> onFinish) {
        Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
        if (sourceClientId != null) {
            clientsForKey.remove(sourceClientId);
        }
        LOGGER.log(Level.FINEST, "putEntry from {0}, key={1}, clientsForKey:{2}", new Object[]{sourceClientId, key, clientsForKey});
        cacheStatus.registerKeyForClient(key, sourceClientId, expiretime);
        if (clientsForKey.isEmpty()) {
            onFinish.onResult(key, null);
            return;
        }
        BroadcastRequestStatus propagation = new BroadcastRequestStatus("putEntry " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, onFinish, null);
        BroadcastRequestStatusMonitor.register(propagation);

        clientsForKey.forEach((clientId) -> {
            CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
            if (connection == null) {
                LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering key " + key + " invalidate");
                propagation.clientDone(clientId);
            } else {
                connection.sendPutEntry(sourceClientId, key, data, expiretime, propagation);
            }
        });
    }

    public void invalidateKey(String key, String sourceClientId, SimpleCallback<String> onFinish) {
        Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
        if (sourceClientId != null) {
            clientsForKey.remove(sourceClientId);
        }
        LOGGER.log(Level.SEVERE, "invalidateKey {0} from {1} interested clients {2}", new Object[]{key, sourceClientId, clientsForKey});
        if (clientsForKey.isEmpty()) {
            onFinish.onResult(key, null);
            return;
        }
        BroadcastRequestStatus invalidation = new BroadcastRequestStatus("invalidateKey " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, onFinish, (clientId, error) -> {
            cacheStatus.removeKeyForClient(key, clientId);
        });
        BroadcastRequestStatusMonitor.register(invalidation);

        clientsForKey.forEach((clientId) -> {
            CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
            if (connection == null) {
                LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering key " + key + " invalidate");
                invalidation.clientDone(clientId);
            } else {
                connection.sendKeyInvalidationMessage(sourceClientId, key, invalidation);
            }
        });

    }

    public void unregisterEntry(String key, String clientId, SimpleCallback<String> onFinish) {
        LOGGER.log(Level.SEVERE, "client " + clientId + " evicted entry " + key);
        cacheStatus.removeKeyForClient(key, clientId);
    }

    public void fetchEntry(String key, String clientId, SimpleCallback<Message> onFinish) {
        LOGGER.log(Level.SEVERE, "client " + clientId + " fetchEntry " + key);
        Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
        clientsForKey.remove(key);
        if (clientsForKey.isEmpty()) {
            onFinish.onResult(Message.ERROR(clientId, new Exception("no client for key " + key)), null);
            return;
        }
        boolean done = false;
        for (String remoteClientId : clientsForKey) {
            CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(remoteClientId);
            if (connection != null) {
                connection.sendFetchKeyMessage(remoteClientId, key, new SimpleCallback<Message>() {

                    @Override
                    public void onResult(Message result, Throwable error) {
                        LOGGER.log(Level.SEVERE, "client " + remoteClientId + " answer to fetch :" + result, error);
                        if (result.type == Message.TYPE_ACK) {
                            // da questo momento consideriamo che il client abbia la entry in memoria
                            // anche se di fatto potrebbe succedere che il messaggio di risposta non arrivi mai
                            long expiretime = (long) result.parameters.get("expiretime");
                            cacheStatus.registerKeyForClient(key, clientId, expiretime);
                        }
                        onFinish.onResult(result, error);
                    }
                });
                done = true;
                break;
            }
        }
        if (!done) {
            onFinish.onResult(Message.ERROR(clientId, new Exception("no connected client for key " + key)), null);
            return;
        }
    }

    public void invalidateByPrefix(String prefix, String sourceClientId, SimpleCallback<String> onFinish) {
        Set<String> clients = cacheStatus.getAllClientsWithListener();
        if (sourceClientId != null) {
            clients.remove(sourceClientId);
        }
        if (clients.isEmpty()) {
            onFinish.onResult(prefix, null);
            return;
        }
        BroadcastRequestStatus invalidation = new BroadcastRequestStatus("invalidateByPrefix " + prefix + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clients, onFinish, (clientId, error) -> {
            cacheStatus.removeKeyByPrefixForClient(prefix, clientId);
        });

        BroadcastRequestStatusMonitor.register(invalidation);
        clients.forEach((clientId) -> {
            CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
            if (connection == null) {
                LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering prefix " + prefix + " invalidate");
                invalidation.clientDone(clientId);
            } else {
                connection.sendPrefixInvalidationMessage(sourceClientId, prefix, invalidation);
            }
        });
    }

    void clientDisconnected(String clientId) {
        int count = cacheStatus.removeClientListeners(clientId);
        LOGGER.log(Level.SEVERE, "client " + clientId + " disconnected, removed " + count + " key listeners");
    }

}
