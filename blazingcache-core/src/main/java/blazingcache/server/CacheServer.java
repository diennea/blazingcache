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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import blazingcache.management.JMXUtils;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyChannelAcceptor;
import blazingcache.server.management.BlazingCacheServerStatusMXBean;
import blazingcache.server.management.CacheServerStatusMXBean;
import blazingcache.zookeeper.LeaderShipChangeListener;
import blazingcache.zookeeper.ZKClusterManager;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * The CacheServer core.
 *
 * @author enrico.olivelli
 */
public class CacheServer implements AutoCloseable {

    private final static Logger LOGGER = Logger.getLogger(CacheServer.class.getName());

    private final String serverId;
    private final String sharedSecret;
    private final CacheServerEndpoint acceptor;
    private final CacheStatus cacheStatus = new CacheStatus();
    private final KeyedLockManager locksManager = new KeyedLockManager();
    private final NettyChannelAcceptor server;
    private final CacheServerStatusMXBean statusMXBean;
    private final AtomicLong pendingOperations;
    private final AtomicInteger connectedClients;
    private final BroadcastRequestStatusMonitor networkRequestsStatusMonitor = new BroadcastRequestStatusMonitor();

    private volatile boolean leader;
    private volatile boolean stopped;
    private ZKClusterManager clusterManager;
    private Thread expireManager;
    private ExecutorService channelsHandlers;
    private int channelHandlersThreads = 64;
    private long stateChangeTimestamp;
    private long slowClientTimeout = 120000;
    private long clientFetchTimeout = 2000;

    public static String VERSION() {
        return "1.6.1-BETA";
    }

    public CacheServer(String sharedSecret, ServerHostData serverHostData) {
        this.sharedSecret = sharedSecret;
        this.acceptor = new CacheServerEndpoint(this);
        this.server = new NettyChannelAcceptor(serverHostData.getHost(), serverHostData.getPort(), serverHostData.isSsl());
        this.server.setAcceptor(acceptor);
        this.leader = true;
        this.serverId = serverHostData.getHost() + "_" + serverHostData.getPort();
        this.statusMXBean = new BlazingCacheServerStatusMXBean(this);
        this.pendingOperations = new AtomicLong();
        this.connectedClients = new AtomicInteger();
    }

    public void setupSsl(File certificateFile, String password, File certificateChain, List<String> sslCiphers) {
        this.server.setSslCertChainFile(certificateChain);
        this.server.setSslCertChainFile(certificateFile);
        this.server.setSslCertPassword(password);
        this.server.setSslCiphers(sslCiphers);
    }

    public int getChannelHandlersThreads() {
        return channelHandlersThreads;
    }

    public void setChannelHandlersThreads(int channelHandlersThreads) {
        this.channelHandlersThreads = channelHandlersThreads;
    }

    public int getCallbackThreads() {
        return server.getCallbackThreads();
    }

    public void setCallbackThreads(int callbackThreads) {
        server.setCallbackThreads(callbackThreads);
    }

    public int getWorkerThreads() {

        return server.getWorkerThreads();
    }

    public void setWorkerThreads(int workerThreads) {
        this.server.setWorkerThreads(workerThreads);
    }

    public CacheStatus getCacheStatus() {
        return cacheStatus;
    }

    void touchEntry(String key, String clientId, long expiretime) {
        cacheStatus.touchKeyFromClient(key, clientId, expiretime);
    }

    private class LeaderShipChangeListenerImpl extends LeaderShipChangeListener {

        @Override
        public void leadershipLost() {
            leader = false;
            CacheServer.this.stateChangeTimestamp = System.currentTimeMillis();
        }

        @Override
        public void leadershipAcquired() {
            leader = true;
            CacheServer.this.stateChangeTimestamp = System.currentTimeMillis();
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
        if (channelHandlersThreads == 0) {
            this.channelsHandlers = Executors.newCachedThreadPool();
        } else {
            this.channelsHandlers = Executors.newFixedThreadPool(channelHandlersThreads, new ThreadFactory() {
                AtomicLong count = new AtomicLong();

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "blazingcache-channel-handler-" + count.incrementAndGet());
                    return t;
                }
            });
        }
        this.expireManager = new Thread(new Expirer(), "cache-server-expire-thread");
        this.expireManager.setDaemon(true);
        this.expireManager.start();
        if (this.server.getPort() > 0) {
            this.server.start();
        }
    }

    private int expirerPeriod = 1000;

    public int getExpirerPeriod() {
        return expirerPeriod;
    }

    public void setExpirerPeriod(int expirerPeriod) {
        this.expirerPeriod = expirerPeriod;
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
                            LOGGER.log(Level.FINER, "expiring entry {0}", key);
                            invalidateKey(key, "expire-timer", null, new SimpleCallback<String>() {

                                @Override
                                public void onResult(String result, Throwable error) {
                                    if (error != null) {
                                        LOGGER.log(Level.SEVERE, "expired entry {0} {1}", new Object[]{key, error});
                                    } else {
                                        LOGGER.log(Level.FINER, " OK" + "expired entry {0}", key);
                                    }
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

                acceptor.processIdleConnections();

                try {
                    Thread.sleep(expirerPeriod);
                } catch (InterruptedException exit) {
                    break;
                }
            }
            LOGGER.log(Level.FINE, "expirer thread stopped");
        }
    }

    void addConnectedClients(final int delta) {
        this.connectedClients.addAndGet(delta);
    }

    void addPendingOperations(final long delta) {
        this.pendingOperations.addAndGet(delta);
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

        channelsHandlers.shutdown();
    }

    public void putEntry(String key, byte[] data, long expiretime, String sourceClientId, String clientProvidedLockId, SimpleCallback<String> onFinish) {
        Runnable action = () -> {
            final LockID lockID = locksManager.acquireWriteLockForKey(key, sourceClientId, clientProvidedLockId);
            if (lockID == null) {
                onFinish.onResult(null, new Exception("invalid clientProvidedLockId " + clientProvidedLockId));
                return;
            }
            Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
            if (sourceClientId != null) {
                clientsForKey.remove(sourceClientId);
            }
            LOGGER.log(Level.FINEST, "putEntry from {0}, key={1}, clientsForKey:{2}", new Object[]{sourceClientId, key, clientsForKey});
            cacheStatus.registerKeyForClient(key, sourceClientId, expiretime);
            SimpleCallback<String> finishAndReleaseLock = new SimpleCallback<String>() {
                @Override
                public void onResult(String result, Throwable error) {
                    locksManager.releaseWriteLockForKey(key, sourceClientId, lockID);
                    onFinish.onResult(result, error);
                }
            };
            if (clientsForKey.isEmpty()) {
                finishAndReleaseLock.onResult(key, null);
                return;
            }
            BroadcastRequestStatus propagation = new BroadcastRequestStatus("putEntry " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, finishAndReleaseLock, null);
            networkRequestsStatusMonitor.register(propagation);

            clientsForKey.forEach((clientId) -> {
                CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
                if (connection == null) {
                    LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering key " + key + " invalidated");
                    propagation.clientDone(clientId);
                } else {
                    connection.sendPutEntry(sourceClientId, key, data, expiretime, propagation);
                }
            });
        };
        executeOnHandler("putEntry " + sourceClientId + "," + key, action);
    }

    public void invalidateKey(String key, String sourceClientId, String clientProvidedLockId, SimpleCallback<String> onFinish) {
        Runnable action = () -> {
            final LockID lockID = locksManager.acquireWriteLockForKey(key, sourceClientId, clientProvidedLockId);
            if (lockID == null) {
                onFinish.onResult(null, new Exception("invalid clientProvidedLockId " + clientProvidedLockId));
                return;
            }
            Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
            if (sourceClientId != null) {
                clientsForKey.remove(sourceClientId);
            }
            SimpleCallback<String> finishAndReleaseLock = new SimpleCallback<String>() {
                @Override
                public void onResult(String result, Throwable error) {
                    locksManager.releaseWriteLockForKey(key, sourceClientId, lockID);
                    onFinish.onResult(result, error);
                }
            };
            if (clientsForKey.isEmpty()) {
                finishAndReleaseLock.onResult(key, null);
                return;
            }
            LOGGER.log(Level.FINE, "invalidateKey {0} from {1} interested clients {2}", new Object[]{key, sourceClientId, clientsForKey});

            BroadcastRequestStatus invalidation = new BroadcastRequestStatus("invalidateKey " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, finishAndReleaseLock, (clientId, error) -> {
                cacheStatus.removeKeyForClient(key, clientId);
            });
            networkRequestsStatusMonitor.register(invalidation);

            clientsForKey.forEach((clientId) -> {
                CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
                if (connection == null) {
                    LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering key " + key + " invalidated");
                    invalidation.clientDone(clientId);
                } else {
                    connection.sendKeyInvalidationMessage(sourceClientId, key, invalidation);
                }
            });
        };
        executeOnHandler("invalidateKey " + sourceClientId + "," + key, action);
    }

    public void lockKey(String key, String sourceClientId, SimpleCallback<String> onFinish) {
        Runnable action = () -> {
            final LockID lockID = locksManager.acquireWriteLockForKey(key, sourceClientId);
            cacheStatus.clientLockedKey(sourceClientId, key, lockID);
            onFinish.onResult(lockID.stamp + "", null);
        };
        executeOnHandler("lockKey " + sourceClientId + "," + key, action);
    }

    public void unlockKey(String key, String sourceClientId, String lockId, SimpleCallback<String> onFinish) {
        Runnable action = () -> {
            LockID lockID = new LockID(Long.parseLong(lockId));
            locksManager.releaseWriteLockForKey(key, lockId, lockID);
            cacheStatus.clientUnlockedKey(sourceClientId, key, lockID);
            onFinish.onResult(lockID.stamp + "", null);
        };
        executeOnHandler("unlockKey " + sourceClientId + "," + key, action);
    }

    public void unregisterEntry(String key, String sourceClientId, SimpleCallback<String> onFinish) {
        LOGGER.log(Level.FINER, "client " + sourceClientId + " evicted entry " + key);
        Runnable action = () -> {
            final LockID lockID = locksManager.acquireWriteLockForKey(key, sourceClientId);
            try {
                cacheStatus.removeKeyForClient(key, sourceClientId);
            } finally {
                locksManager.releaseWriteLockForKey(key, sourceClientId, lockID);
            }
            onFinish.onResult(null, null);
        };
        executeOnHandler("unregisterEntry " + sourceClientId + "," + key, action);
    }

    public void fetchEntry(String key, String clientId, String clientProvidedLockId, SimpleCallback<Message> onFinish) {
        Runnable action = () -> {
            final LockID lockID = locksManager.acquireWriteLockForKey(key, clientId, clientProvidedLockId);
            if (lockID == null) {
                onFinish.onResult(null, new Exception("invalid clientProvidedLockId " + clientProvidedLockId));
                return;
            }
            Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
            if (clientId != null) {
                clientsForKey.remove(clientId);
            }
            LOGGER.log(Level.FINE, "client {0} fetchEntry {1} ask to {2}", new Object[]{clientId, key, clientsForKey});
            SimpleCallback<Message> finishAndReleaseLock = new SimpleCallback<Message>() {
                @Override
                public void onResult(Message result, Throwable error) {
                    locksManager.releaseWriteLockForKey(key, clientId, lockID);
                    onFinish.onResult(result, error);
                }
            };
            if (clientsForKey.isEmpty()) {
                finishAndReleaseLock.onResult(Message.ERROR(clientId, new Exception("no client for key " + key)), null);
                return;
            }

            List<CacheServerSideConnection> candidates = new ArrayList<>();
            for (String remoteClientId : clientsForKey) {
                CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(remoteClientId);
                if (connection != null && connection.getFetchPriority() > 0) {
                    candidates.add(connection);
                }
            }
            candidates.sort((a, b) -> {
                return b.getFetchPriority() - a.getFetchPriority();
            });            

            boolean foundOneGoodClientConnected = false;
            for (CacheServerSideConnection connection : candidates) {
                String remoteClientId = connection.getClientId();

                UnicastRequestStatus unicastRequestStatus = new UnicastRequestStatus(clientId, remoteClientId, "fetch " + key);
                networkRequestsStatusMonitor.register(unicastRequestStatus);
                connection.sendFetchKeyMessage(remoteClientId, key, new SimpleCallback<Message>() {

                    @Override
                    public void onResult(Message result, Throwable error) {
                        networkRequestsStatusMonitor.unregister(unicastRequestStatus);
                        LOGGER.log(Level.FINE, "client " + remoteClientId + " answer to fetch :" + result, error);
                        if (result.type == Message.TYPE_ACK) {
                            // da questo momento consideriamo che il client abbia la entry in memoria
                            // anche se di fatto potrebbe succedere che il messaggio di risposta non arrivi mai
                            long expiretime = (long) result.parameters.get("expiretime");
                            cacheStatus.registerKeyForClient(key, clientId, expiretime);
                        }
                        finishAndReleaseLock.onResult(result, error);
                    }
                });
                foundOneGoodClientConnected = true;
                break;
            }
            if (!foundOneGoodClientConnected) {
                finishAndReleaseLock.onResult(Message.ERROR(clientId, new Exception("no connected client for key " + key)), null);
            }
        };
        executeOnHandler("fetchEntry " + clientId + "," + key, action);
    }

    public void invalidateByPrefix(String prefix, String sourceClientId, SimpleCallback<String> onFinish) {
        Runnable action = () -> {
            // LOCKS ??
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

            networkRequestsStatusMonitor.register(invalidation);
            clients.forEach((clientId) -> {
                CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(clientId);
                if (connection == null) {
                    LOGGER.log(Level.SEVERE, "client " + clientId + " not connected, considering prefix " + prefix + " invalidated");
                    invalidation.clientDone(clientId);
                } else {
                    connection.sendPrefixInvalidationMessage(sourceClientId, prefix, invalidation);
                }
            });
        };
        executeOnHandler("invalidateByPrefix " + prefix, action);
    }

    private void executeOnHandler(String name, Runnable runnable) {
        try {
            channelsHandlers.submit(new ManagedRunnable(name, runnable));
        } catch (RejectedExecutionException rejected) {
            LOGGER.log(Level.SEVERE, "rejected execution of " + name + ":" + rejected, rejected);
        }
    }

    void clientDisconnected(String clientId) {
        CacheStatus.ClientRemovalResult removalResult = cacheStatus.removeClientListeners(clientId);
        int count = removalResult.getListenersCount();
        Map<String, List<LockID>> locks = removalResult.getLocks();
        LOGGER.log(Level.SEVERE, "client " + clientId + " disconnected, removed " + count + " key listeners, locks:" + locks);
        if (locks != null) {
            locks.forEach((key, locksForKey) -> {
                locksForKey.forEach(lock -> {
                    locksManager.releaseWriteLockForKey(key, clientId, lock);
                });

            });
        }
    }

    public long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    public long getStateChangeTimestamp() {
        return this.stateChangeTimestamp;
    }

    public int getGlobalCacheSize() {
        return this.cacheStatus.getTotalEntryCount();
    }

    public int getNumberOfConnectedClients() {
        return this.connectedClients.get();
    }

    public long getPendingOperations() {
        return this.pendingOperations.get();
    }

    public String getServerId() {
        return this.serverId;
    }

    public KeyedLockManager getLocksManager() {
        return locksManager;
    }

    public int getNumberOfLockedKeys() {
        return this.locksManager.getNumberOfLockedKeys();
    }

    public long getSlowClientTimeout() {
        return slowClientTimeout;
    }

    public void setSlowClientTimeout(long slowClientTimeout) {
        this.slowClientTimeout = slowClientTimeout;
    }

    public long getClientFetchTimeout() {
        return clientFetchTimeout;
    }

    public void setClientFetchTimeout(long clientFetchTimeout) {
        this.clientFetchTimeout = clientFetchTimeout;
    }

    /**
     * Register the status mbean related to this server if the input param is
     * true.
     * <p>
     * If the param is false, the status mbean would not be enabled.
     *
     * @param enabled true in order to enable publishing on JMX
     */
    public void enableJmx(final boolean enabled) {
        if (enabled) {
            JMXUtils.registerServerStatusMXBean(this, statusMXBean);
        } else {
            JMXUtils.unregisterServerStatusMXBean(this);
        }
    }

    /**
     * Access lowlevel information about pending network requests
     *
     * @return
     */
    public BroadcastRequestStatusMonitor getNetworkRequestsStatusMonitor() {
        return networkRequestsStatusMonitor;
    }

}
