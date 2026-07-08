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

import blazingcache.management.JMXUtils;
import blazingcache.network.Message;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyChannelAcceptor;
import blazingcache.server.management.BlazingCacheServerStatusMXBean;
import blazingcache.server.management.CacheServerStatusMXBean;
import blazingcache.utils.RawString;
import blazingcache.zookeeper.LeaderShipChangeListener;
import blazingcache.zookeeper.ZKClusterManager;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.zookeeper.ZooKeeper;

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
    private final KeyedScheduler scheduler = new KeyedScheduler();
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
    private final long lastStartupTimestamp = System.currentTimeMillis();
    private boolean requireAuthentication = true;

    public static String VERSION() {
        return "1.14.0-SNAPSHOT";
    }

    public boolean isRequireAuthentication() {
        return requireAuthentication;
    }

    public void setRequireAuthentication(boolean requireAuthentication) {
        this.requireAuthentication = requireAuthentication;
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
        this.server.setSslCertFile(certificateFile);
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

    void touchEntry(RawString key, String clientId, long expiretime) {
        cacheStatus.touchKeyFromClient(key, clientId, expiretime);
    }

    private class LeaderShipChangeListenerImpl extends LeaderShipChangeListener {

        @Override
        public void leadershipLost() {
            leader = false;
            CacheServer.this.stateChangeTimestamp = System.currentTimeMillis();
            //close currently active client connections
            acceptor.closeAllClientConnections();
        }

        @Override
        public void leadershipAcquired() {
            leader = true;
            CacheServer.this.stateChangeTimestamp = System.currentTimeMillis();
        }

    }

    public void setupCluster(
        String zkAddress, int zkTimeout, String basePath, ServerHostData localhostdata, boolean writeacls) throws Exception {
        leader = false;
        clusterManager = new ZKClusterManager(zkAddress, zkTimeout, basePath, new LeaderShipChangeListenerImpl(), ServerHostData.formatHostdata(localhostdata), writeacls);
        clusterManager.start();
        clusterManager.requestLeadership();
    }

    public void start() throws Exception {
        JVMServersRegistry.registerServer(serverId, this);
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
                    List<RawString> entries = cacheStatus.selectExpiredEntries(now, 1000);
                    if (!entries.isEmpty()) {

                        CountDownLatch latch = new CountDownLatch(entries.size());
                        for (RawString key : entries) {
                            LOGGER.log(Level.FINER, "expiring entry {0}", key);
                            invalidateKey(key, "expire-timer", null, new SimpleCallback<RawString>() {

                                @Override
                                public void onResult(RawString result, Throwable error) {
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
        JVMServersRegistry.unregisterServer(serverId);
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

    public void putEntry(RawString key, byte[] data, long expiretime, String sourceClientId, String clientProvidedLockId, SimpleCallback<RawString> onFinish) {
        // Registering the source as a holder is the always-applied side effect (kept
        // even when the broadcast is coalesced away); the broadcast pushes the new
        // value to the other holders.
        Runnable registration = () -> cacheStatus.registerKeyForClient(key, sourceClientId, expiretime);
        Consumer<Runnable> broadcast = (onComplete) -> {
            Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
            if (sourceClientId != null) {
                clientsForKey.remove(sourceClientId);
            }
            LOGGER.log(Level.FINEST, "putEntry from {0}, key={1}, clientsForKey:{2}", new Object[]{sourceClientId, key, clientsForKey});
            if (clientsForKey.isEmpty()) {
                onComplete.run();
                return;
            }
            BroadcastRequestStatus propagation = new BroadcastRequestStatus("putEntry " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, (result, error) -> onComplete.run(), null);
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
        scheduler.submitExclusive(key, KeyedScheduler.Verb.PUT, clientProvidedLockId, registration, broadcast, key, onFinish);
    }

    public void loadEntry(RawString key, long expiretime, String sourceClientId, String clientProvidedLockId, SimpleCallback<RawString> onFinish) {
        LOGGER.log(Level.FINEST, "loadEntry from {0}, key={1}", new Object[]{sourceClientId, key});
        // load is local-only: it registers the source as a holder and does not
        // broadcast anything to the other clients.
        Runnable registration = () -> cacheStatus.registerKeyForClient(key, sourceClientId, expiretime);
        scheduler.submitExclusive(key, KeyedScheduler.Verb.LOAD, clientProvidedLockId, registration, null, key, onFinish);
    }

    public void invalidateKey(RawString key, String sourceClientId, String clientProvidedLockId, SimpleCallback<RawString> onFinish) {
        // Removing the source as a holder is the always-applied side effect (kept even
        // when the broadcast is coalesced away); the broadcast tells the other holders
        // to drop the entry. Coalescing of concurrent/queued invalidations (issue #188)
        // is handled by the scheduler: a plain invalidation carries no client lock and
        // is therefore coalescible, while one carrying an explicit lock is not.
        Runnable registration = () -> cacheStatus.removeKeyForClient(key, sourceClientId);
        Consumer<Runnable> broadcast = (onComplete) -> broadcastInvalidation(key, sourceClientId, (result, error) -> onComplete.run());
        scheduler.submitExclusive(key, KeyedScheduler.Verb.INVALIDATE, clientProvidedLockId, registration, broadcast, key, onFinish);
    }

    /**
     * Snapshots the clients holding the key (excluding the source) and broadcasts
     * an invalidation to them; {@code onFinish} is invoked once every interested
     * client has acknowledged (or been considered invalidated). Must be called
     * while the scheduler holds the per-key exclusive slot for {@code key}.
     */
    private void broadcastInvalidation(RawString key, String sourceClientId, SimpleCallback<RawString> onFinish) {
        Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
        if (sourceClientId != null) {
            clientsForKey.remove(sourceClientId);
        }
        if (clientsForKey.isEmpty()) {
            onFinish.onResult(key, null);
            return;
        }
        LOGGER.log(Level.FINE, "invalidateKey {0} from {1} interested clients {2}", new Object[]{key, sourceClientId, clientsForKey});

        BroadcastRequestStatus invalidation = new BroadcastRequestStatus("invalidateKey " + key + " from " + sourceClientId + " started at " + new java.sql.Timestamp(System.currentTimeMillis()), clientsForKey, onFinish, (clientId, error) -> {
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
    }

    public void lockKey(RawString key, String sourceClientId, CacheServerSideConnection connection, SimpleCallback<String> onFinish) {
        // The scheduler owns the lock lifecycle. At grant time it verifies the lock is
        // still owned by the SAME connection that requested it, not merely that the
        // client id is still connected: a LOCK that raced ahead of its connection's
        // disconnect cleanup must not be granted to a *new* connection of the same
        // client that reconnected in the meantime — the grant reply would be sent on the
        // original (dead) channel and silently dropped, stalling the key forever.
        scheduler.submitLock(key, sourceClientId,
                () -> acceptor.getActualConnectionFromClient(sourceClientId) == connection,
                onFinish);
    }

    public void unlockKey(RawString key, String sourceClientId, String lockId, SimpleCallback<String> onFinish) {
        // A malformed lockId must not leave the caller hanging.
        Long token = KeyedScheduler.parseProvidedLockId(lockId);
        if (token == null) {
            onFinish.onResult(null, new Exception("invalid lockId " + lockId));
            return;
        }
        scheduler.submitUnlock(key, token, onFinish);
    }

    public void unregisterEntries(final List<RawString> keys, String sourceClientId, SimpleCallback<RawString> onFinish) {
        LOGGER.log(Level.FINER, "client {0} evicted entries {1}", new Object[]{sourceClientId, keys});
        if (keys.isEmpty()) {
            onFinish.onResult(null, null);
            return;
        }
        // Route each eviction through the scheduler as a local (broadcast-less)
        // exclusive op so it is serialized with the other work on the same key; reply
        // once every key has been processed.
        AtomicInteger remaining = new AtomicInteger(keys.size());
        SimpleCallback<RawString> perKey = (result, error) -> {
            if (remaining.decrementAndGet() == 0) {
                onFinish.onResult(null, null);
            }
        };
        for (RawString key : keys) {
            Runnable registration = () -> cacheStatus.removeKeyForClient(key, sourceClientId);
            scheduler.submitExclusive(key, KeyedScheduler.Verb.LOAD, null, registration, null, key, perKey);
        }
    }

    public void fetchEntry(RawString key, String clientId, String clientProvidedLockId, SimpleCallback<Message> onFinish) {
        // A fetch is a SHARED operation: concurrent fetches on the same key run in
        // parallel, while still being mutually exclusive with invalidate/put/load
        // (exclusive), preserving the ordering invariant that the fetch
        // client-registration must not race an invalidate.
        // The remote fetch reply callback can fire more than once (send failure then
        // reply timeout, see NettyChannel.sendMessageWithAsyncReply); guard so the slot
        // release and the client reply (and its pending-operations bookkeeping) happen
        // exactly once.
        AtomicBoolean finished = new AtomicBoolean();
        Consumer<Runnable> body = (onComplete) -> {
            // Register the fetching client (on success) BEFORE releasing the shared
            // slot, then reply: a subsequent invalidate can only start once the slot is
            // released, so it always sees the registration and removes it.
            SimpleCallback<Message> finish = (result, error) -> {
                if (finished.compareAndSet(false, true)) {
                    onComplete.run();
                    onFinish.onResult(result, error);
                }
            };
            try {
                Set<String> clientsForKey = cacheStatus.getClientsForKey(key);
                if (clientId != null) {
                    clientsForKey.remove(clientId);
                }
                LOGGER.log(Level.FINE, "client {0} fetchEntry {1} ask to {2}", new Object[]{clientId, key, clientsForKey});
                if (clientsForKey.isEmpty()) {
                    finish.onResult(Message.ERROR(clientId, new Exception("no client for key " + key)), null);
                    return;
                }

                int maxPriority = 0;
                List<CacheServerSideConnection> maxPriorityCandidates = new ArrayList<>();
                for (String remoteClientId : clientsForKey) {
                    CacheServerSideConnection connection = acceptor.getActualConnectionFromClient(remoteClientId);
                    if (connection != null) {
                        int fetchPriority = connection.getFetchPriority();
                        if (fetchPriority == 0 || fetchPriority < maxPriority) {
                            continue;
                        }

                        if (fetchPriority > maxPriority) {
                            maxPriorityCandidates.clear();
                            maxPriority = fetchPriority;
                        }
                        maxPriorityCandidates.add(connection);
                    }
                }

                if (maxPriorityCandidates.isEmpty()) {
                    finish.onResult(Message.ERROR(clientId, new Exception("no connected client for key " + key)), null);
                    return;
                }

                CacheServerSideConnection connection = maxPriorityCandidates.get(ThreadLocalRandom.current().nextInt(maxPriorityCandidates.size()));
                String remoteClientId = connection.getClientId();
                UnicastRequestStatus unicastRequestStatus = new UnicastRequestStatus(clientId, remoteClientId, "fetch " + key);
                networkRequestsStatusMonitor.register(unicastRequestStatus);

                connection.sendFetchKeyMessage(remoteClientId, key, (result, error) -> {
                    networkRequestsStatusMonitor.unregister(unicastRequestStatus);
                    LOGGER.log(Level.FINE, "client " + remoteClientId + " answer to fetch :" + result, error);
                    if (result != null && result.type == Message.TYPE_ACK) {
                        // da questo momento consideriamo che il client abbia la entry in memoria
                        // anche se di fatto potrebbe succedere che il messaggio di risposta non arrivi mai
                        long expiretime = (long) result.parameters.get("expiretime");
                        cacheStatus.registerKeyForClient(key, clientId, expiretime);
                    }
                    finish.onResult(result, error);
                });
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error in fetchEntry for key " + key + ", releasing the slot", t);
                finish.onResult(Message.ERROR(clientId, t), null);
            }
        };
        scheduler.submitFetch(key, clientProvidedLockId, body,
                () -> onFinish.onResult(null, new Exception("invalid clientProvidedLockId " + clientProvidedLockId)));
    }

    public void invalidateByPrefix(RawString prefix, String sourceClientId, SimpleCallback<RawString> onFinish) {
        Runnable action = () -> {
            // Enumerate the keys matching the prefix and run a per-key invalidation
            // through the scheduler, so each one is serialized (on its own key slot) with
            // any concurrent put/load/fetch on that key. A single bulk prefix removal
            // (the previous approach) did not go through the scheduler and could
            // interleave with an in-flight putEntry on a matching key: the client would
            // re-store the entry after dropping it while the server had already removed
            // its registration, leaving the client holding stale data the server no
            // longer knows about.
            List<RawString> keys = new ArrayList<>();
            for (RawString key : cacheStatus.getKeys()) {
                if (key.startsWith(prefix)) {
                    keys.add(key);
                }
            }
            if (keys.isEmpty()) {
                onFinish.onResult(prefix, null);
                return;
            }
            AtomicInteger remaining = new AtomicInteger(keys.size());
            SimpleCallback<RawString> perKey = (result, error) -> {
                if (remaining.decrementAndGet() == 0) {
                    onFinish.onResult(prefix, null);
                }
            };
            for (RawString key : keys) {
                invalidateKey(key, sourceClientId, null, perKey);
            }
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
        int count = cacheStatus.removeClientListeners(clientId);
        LOGGER.log(Level.SEVERE, "client " + clientId + " disconnected, removed " + count + " key listeners");
        // Release every application lock the client held or was queued for. This is
        // driven entirely by the scheduler (which owns the lock ownership), so a lock
        // being acquired or still queued when the client dropped is not left dangling.
        scheduler.releaseLocksForClient(clientId);
    }

    public long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    public long getStateChangeTimestamp() {
        return this.stateChangeTimestamp;
    }

    public long getLastStartupTimestamp() {
        return lastStartupTimestamp;
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

    public KeyedScheduler getLocksManager() {
        return scheduler;
    }

    public int getNumberOfLockedKeys() {
        return this.scheduler.getNumberOfLockedKeys();
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
     * Register the status mbean related to this server if the input param is true.
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
     * This method should be used only for debug purposes. Return ZooKeeper client if clustering mode (ZooKeeper-based)
     * is on.
     *
     * @return the ZooKeeper client exploited by this CacheServer is clustering mode is on, null otherwise
     */
    ZooKeeper getZooKeeper() {
        if (this.clusterManager != null) {
            return this.clusterManager.getZooKeeper();
        } else {
            return null;
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
