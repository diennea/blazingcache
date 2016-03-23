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
package blazingcache.client;

import blazingcache.client.impl.InternalClientListener;
import blazingcache.client.impl.PendingFetchesManager;
import blazingcache.client.management.BlazingCacheClientStatisticsMXBean;
import blazingcache.client.management.BlazingCacheClientStatusMXBean;
import blazingcache.client.management.CacheClientStatisticsMXBean;
import blazingcache.client.management.CacheClientStatusMXBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.ConnectionRequestInfo;
import blazingcache.network.Message;
import blazingcache.network.ReplyCallback;
import blazingcache.network.SendResultCallback;
import blazingcache.network.ServerLocator;
import blazingcache.network.ServerNotAvailableException;
import blazingcache.network.ServerRejectedConnectionException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Client.
 *
 * @author enrico.olivelli
 */
public class CacheClient implements ChannelEventListener, ConnectionRequestInfo, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(CacheClient.class.getName());

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ServerLocator brokerLocator;
    private final Thread coreThread;
    private final String clientId;
    private final String sharedSecret;
    private final CacheClientStatisticsMXBean statisticsMXBean;
    private final CacheClientStatusMXBean statusMXBean;

    private volatile boolean stopped = false;
    private Channel channel;
    private long connectionTimestamp;
    private int fetchPriority = 10;

    private final AtomicLong oldestEvictedKeyAge;
    private final AtomicLong clientPuts;
    private final AtomicLong clientTouches;
    private final AtomicLong clientGets;
    private final AtomicLong clientFetches;
    private final AtomicLong clientEvictions;
    private final AtomicLong clientInvalidations;
    private final AtomicLong clientHits;
    private final AtomicLong clientMissedGetsToSuccessfulFetches;
    private final AtomicLong clientMissedGetsToMissedFetches;

    /**
     * Maximum amount of memory used for storing entry values. 0 or negative to
     * disable
     */
    private long maxMemory = 0;

    /**
     * Maximum amount of memory used for storing entry values. 0 or negative to
     * disable.
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Maximum amount of memory used for storing entry values. 0 or negative to
     * disable
     */
    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    @Override
    public int getFetchPriority() {
        return fetchPriority;
    }

    /**
     * Assign a priority to be used when a client is to be choosen for serving a
     * remote fetch. Setting fetchPriority to 0 will prevent this client from
     * being asked to serve fetch requests from other clients
     *
     * @param fetchPriority
     */
    public void setFetchPriority(int fetchPriority) {
        this.fetchPriority = fetchPriority;
    }

    private final AtomicLong actualMemory = new AtomicLong();

    private InternalClientListener internalClientListener;

    InternalClientListener getInternalClientListener() {
        return internalClientListener;
    }

    void setInternalClientListener(InternalClientListener internalClientListener) {
        this.internalClientListener = internalClientListener;
    }

    public long getActualMemory() {
        return actualMemory.get();
    }

    public long getOldestEvictedKeyAge() {
        return this.oldestEvictedKeyAge.get();
    }

    public String getStatus() {
        Channel _channel = channel;
        if (_channel != null) {
            return "CONNECTED";
        } else {
            return "DISCONNECTED";
        }
    }

    public CacheClient(String clientId, String sharedSecret, ServerLocator brokerLocator) {
        this.brokerLocator = brokerLocator;
        this.sharedSecret = sharedSecret;
        this.coreThread = new Thread(new ConnectionManager(), "cache-connection-manager-" + clientId);
        this.coreThread.setDaemon(true);
        this.clientId = clientId + "_" + System.nanoTime();

        this.statisticsMXBean = new BlazingCacheClientStatisticsMXBean(this);
        this.statusMXBean = new BlazingCacheClientStatusMXBean(this);

        this.oldestEvictedKeyAge = new AtomicLong();
        this.clientPuts = new AtomicLong();
        this.clientTouches = new AtomicLong();
        this.clientGets = new AtomicLong();
        this.clientFetches = new AtomicLong();
        this.clientEvictions = new AtomicLong();
        this.clientInvalidations = new AtomicLong();
        this.clientHits = new AtomicLong();
        this.clientMissedGetsToSuccessfulFetches = new AtomicLong();
        this.clientMissedGetsToMissedFetches = new AtomicLong();
    }

    /**
     * Resets client cache's statistics.
     */
    public void clearStatistics() {
        this.clientPuts.set(0);
        this.clientTouches.set(0);
        this.clientGets.set(0);
        this.clientFetches.set(0);
        this.clientEvictions.set(0);
        this.clientInvalidations.set(0);
        this.clientHits.set(0);
        this.clientMissedGetsToSuccessfulFetches.set(0);
        this.clientMissedGetsToMissedFetches.set(0);
    }

    public ServerLocator getBrokerLocator() {
        return brokerLocator;
    }

    /**
     * Start the client. You MUST start the client before using it, otherwise
     * the client will always operated in disconnected mode
     *
     * @see #isConnected()
     * @see #waitForConnection(int)
     */
    public void start() {
        this.coreThread.start();
    }

    /**
     * Waits for the client to establish the first connection to the server.
     *
     * @param timeout
     * @return
     * @throws InterruptedException
     */
    public boolean waitForConnection(int timeout) throws InterruptedException {
        long time = System.currentTimeMillis();
        while (System.currentTimeMillis() - time <= timeout) {
            if (channel != null) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    /**
     * Waits for the client to be disconnected.
     *
     * @param timeout
     * @return
     * @throws InterruptedException
     */
    public boolean waitForDisconnection(int timeout) throws InterruptedException {
        long time = System.currentTimeMillis();
        while (System.currentTimeMillis() - time <= timeout) {
            if (channel == null) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    /**
     * Returns true if the client is currently connected to the server.
     *
     * @return true if the client is connected to the server; false otherwise
     */
    public boolean isConnected() {
        return channel != null;
    }

    /**
     * Returns the timestamp in ms of the last successful connection to the
     * server.
     * <p>
     * In case of the client being currently disconnected, the value returned
     * will be 0.
     *
     * @return the timestamp of the last successful connection to the server
     */
    public long getConnectionTimestamp() {
        return connectionTimestamp;
    }

    /**
     * Return the current client timestamp in ms.
     *
     * @return the current client timestamp
     */
    public long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * Actual number of entries in the local cache.
     *
     * @return the number of entry stored in the local cache
     */
    public int getCacheSize() {
        return this.cache.size();
    }

    private void connect() throws InterruptedException, ServerNotAvailableException, ServerRejectedConnectionException {
        if (channel != null) {
            try {
                channel.close();
            } finally {
                channel = null;
            }
        }
        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "connecting, clientId=" + this.clientId);
        disconnect();
        channel = brokerLocator.connect(this, this);
        connectionTimestamp = System.currentTimeMillis();
        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "connected, channel:" + channel);
        if (internalClientListener != null) {
            internalClientListener.onConnection(channel);
        }
    }

    /**
     * Disconnects the client. This operation autmatically evicts all the
     * entries from the local cache
     */
    public void disconnect() {
        try {
            this.cache.clear();
            actualMemory.set(0);
            connectionTimestamp = 0;
            Channel c = channel;
            if (c != null) {
                channel = null;
                c.close();
            }
        } finally {
            channel = null;
        }
    }

    private static final Logger CONNECTION_MANAGER_LOGGER = Logger.getLogger(CacheClient.ConnectionManager.class.getName().replace("$", "."));

    private final class ConnectionManager implements Runnable {

        @Override
        public void run() {

            while (!stopped) {
                try {
                    try {
                        Channel _channel = channel;
                        if (_channel == null || !_channel.isValid()) {
                            connect();
                        }
                    } catch (InterruptedException exit) {
                        continue;
                    } catch (ServerNotAvailableException | ServerRejectedConnectionException retry) {
                        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "no broker available:" + retry);
                    }

                    if (channel == null) {
                        try {
                            CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "not connected, waiting 2000 ms");
                            Thread.sleep(2000);
                        } catch (InterruptedException exit) {
                        }
                        continue;
                    }
                    if (maxMemory > 0) {
                        try {
                            ensureMaxMemoryLimit();
                        } catch (InterruptedException exit) {
                            continue;
                        }
                    }

                    Channel _channel = channel;
                    if (_channel != null) {
                        _channel.channelIdle();
                    }

                    try {
                        // TODO: wait for IO error or stop condition before reconnect 
                        CONNECTION_MANAGER_LOGGER.log(Level.FINEST, "connected");
                        Thread.sleep(2000);
                    } catch (InterruptedException exit) {
                        continue;
                    }

                } catch (Throwable t) {
                    CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "unhandled error", t);
                    continue;
                }
            }

            CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "shutting down " + clientId);

            Channel _channel = channel;
            if (_channel != null) {
                _channel.sendOneWayMessage(Message.CLIENT_SHUTDOWN(clientId), new SendResultCallback() {

                    @Override
                    public void messageSent(Message originalMessage, Throwable error) {
                        // ignore
                    }
                });
                disconnect();
            }
        }

    }

    private void ensureMaxMemoryLimit() throws InterruptedException {
        long delta = maxMemory - actualMemory.longValue();
        if (delta > 0) {
            return;
        }
        long to_release = -delta;
        LOGGER.log(Level.SEVERE, "trying to release {0} bytes", to_release);
        List<CacheEntry> evictable = new ArrayList<>();
        java.util.function.Consumer<CacheEntry> accumulator = new java.util.function.Consumer<CacheEntry>() {
            long releasedMemory = 0;

            @Override
            public void accept(CacheEntry t) {
                if (releasedMemory < to_release) {
                    LOGGER.log(Level.FINEST, "evaluating {0} {1} size {2}", new Object[]{t.getKey(), t.getLastGetTime(), t.getSerializedData().length});
                    evictable.add(t);
                    releasedMemory += t.getSerializedData().length;
                }
            }
        };
        try {
            cache.values().stream().sorted(
                    new Comparator<CacheEntry>() {

                @Override
                public int compare(CacheEntry o1, CacheEntry o2) {
                    long diff = o1.getLastGetTime() - o2.getLastGetTime();
                    if (diff == 0) {
                        return 0;
                    }
                    return diff > 0 ? 1 : -1;
                }
            }
            ).forEachOrdered(accumulator);
        } catch (Exception dataChangedDuringSort) {
            LOGGER.severe("dataChangedDuringSort: " + dataChangedDuringSort);
            return;
        }
        LOGGER.severe("found " + evictable.size() + " evictable entries");

        if (!evictable.isEmpty()) {
            //update the age of the oldest evicted key
            //the oldest one is the first entry in evictable
            this.oldestEvictedKeyAge.getAndSet(System.nanoTime() - evictable.get(0).getPutTime());

            CountDownLatch count = new CountDownLatch(evictable.size());
            for (final CacheEntry entry : evictable) {
                final String key = entry.getKey();
                LOGGER.log(Level.SEVERE, "evict {0} size {1} bytes lastAccessDate {2}", new Object[]{key, entry.getSerializedData().length, entry.getLastGetTime()});

                final CacheEntry removed = cache.remove(key);
                if (removed != null) {
                    this.clientEvictions.incrementAndGet();
                    actualMemory.addAndGet(-removed.getSerializedData().length);
                    final Channel _channel = channel;
                    if (_channel != null) {
                        _channel.sendMessageWithAsyncReply(Message.UNREGISTER_ENTRY(clientId, key), invalidateTimeout, new ReplyCallback() {

                            @Override
                            public void replyReceived(Message originalMessage, Message message, Throwable error) {
                                if (error != null) {
                                    LOGGER.log(Level.SEVERE, "error while unregistering entry " + key, error);
                                }
                                count.countDown();
                            }
                        });
                    } else {
                        count.countDown();
                    }
                } else {
                    count.countDown();
                }
            }

            int countWait = 0;
            while (true) {
                LOGGER.log(Level.SEVERE, "waiting for evict ack from server (#{0})", countWait);
                boolean done = count.await(1, TimeUnit.SECONDS);
                if (done) {
                    break;
                }
                final Channel _channel = channel;
                if (_channel == null || !_channel.isValid()) {
                    LOGGER.log(Level.SEVERE, "channel closed during eviction");
                    break;
                }
                countWait++;
            }
            LOGGER.log(Level.SEVERE, "eviction finished");
        }
    }

    @Override
    public void messageReceived(Message message) {
        if (internalClientListener != null) {
            // hook for tests
            boolean proceed = internalClientListener.messageReceived(message, channel);
            if (!proceed) {
                return;
            }
        }
        LOGGER.log(Level.FINER, "{0} messageReceived {1}", new Object[]{clientId, message});
        switch (message.type) {
            case Message.TYPE_INVALIDATE: {
                String key = (String) message.parameters.get("key");
                LOGGER.log(Level.FINEST, clientId + " invalidate " + key + " from " + message.clientId);
                runningFetches.cancelFetchesForKey(key);
                CacheEntry removed = cache.remove(key);
                if (removed != null) {
                    actualMemory.addAndGet(-removed.getSerializedData().length);
                }
                Channel _channel = channel;
                if (_channel != null) {
                    _channel.sendReplyMessage(message, Message.ACK(clientId));
                }
            }
            break;
            case Message.TYPE_INVALIDATE_BY_PREFIX: {
                String prefix = (String) message.parameters.get("prefix");
                LOGGER.log(Level.FINEST, "{0} invalidateByPrefix {1} from {2}", new Object[]{clientId, prefix, message.clientId});
                Collection<String> keys = cache.keySet().stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
                keys.forEach((key) -> {
                    runningFetches.cancelFetchesForKey(key);
                    CacheEntry removed = cache.remove(key);
                    if (removed != null) {
                        actualMemory.addAndGet(-removed.getSerializedData().length);
                    }
                });
                Channel _channel = channel;
                if (_channel != null) {
                    _channel.sendReplyMessage(message, Message.ACK(clientId));
                }
            }
            break;

            case Message.TYPE_PUT_ENTRY: {
                String key = (String) message.parameters.get("key");
                runningFetches.cancelFetchesForKey(key);
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                LOGGER.log(Level.FINEST, "{0} put {1} from {2}", new Object[]{clientId, key, message.clientId});
                CacheEntry cacheEntry = new CacheEntry(key, System.nanoTime(), data, expiretime);
                CacheEntry previous = cache.put(key, cacheEntry);
                if (previous != null) {
                    actualMemory.addAndGet(-previous.getSerializedData().length);
                }
                actualMemory.addAndGet(data.length);
                Channel _channel = channel;
                if (_channel != null) {
                    _channel.sendReplyMessage(message, Message.ACK(clientId));
                }

            }
            break;
            case Message.TYPE_FETCH_ENTRY: {
                String key = (String) message.parameters.get("key");
                CacheEntry entry = cache.get(key);
                LOGGER.log(Level.FINEST, "{0} fetch {1} from {2} -> {3}", new Object[]{clientId, key, message.clientId, entry});
                Channel _channel = channel;
                if (_channel != null) {
                    if (entry != null) {
                        _channel.sendReplyMessage(message,
                                Message.ACK(clientId)
                                .setParameter("data", entry.getSerializedData())
                                .setParameter("expiretime", entry.getExpiretime())
                        );
                    } else {
                        _channel.sendReplyMessage(message,
                                Message.ERROR(clientId, new Exception("entry " + key + " no more here"))
                        );
                    }
                }

            }
            break;
        }
    }

    @Override
    public void channelClosed() {
        LOGGER.log(Level.SEVERE, "channel closed, clearing nearcache");
        cache.clear();
        actualMemory.set(0);
    }

    @Override
    public String getSharedSecret() {
        return sharedSecret;
    }

    /**
     * Closes the client. It will never try to reconnect again to the server
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        stop();
    }

    public void stop() {
        LOGGER.log(Level.SEVERE, "stopping");
        stopped = true;
        try {
            coreThread.interrupt();
            coreThread.join();
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "stop interrupted", ex);
        }
        brokerLocator.close();
    }

    /**
     * Returns an entry from the local cache, if not found asks to the
     * CacheServer to find the entry on other clients.
     *
     * @param key
     * @return
     * @throws InterruptedException
     * @see #get(java.lang.String)
     */
    public CacheEntry fetch(String key) throws InterruptedException {
        return fetch(key, null);
    }

    private final PendingFetchesManager runningFetches = new PendingFetchesManager();

    /**
     * Returns an entry from the local cache, if not found asks the CacheServer
     * to find the entry on other clients.
     *
     * @param key
     * @param lock previouly acquired lock
     * @return
     * @throws InterruptedException
     * @see #get(java.lang.String)
     * @see #lock(java.lang.String)
     */
    public CacheEntry fetch(String key, KeyLock lock) throws InterruptedException {
        long fetchId = runningFetches.registerFetchForKey(key);
        Channel _channel = channel;
        if (_channel == null) {
            LOGGER.log(Level.SEVERE, "fetch failed {0}, not connected", key);
            return null;
        }
        CacheEntry entry = cache.get(key);
        this.clientFetches.incrementAndGet();
        if (entry != null) {
            entry.setLastGetTime(System.nanoTime());
            this.clientHits.incrementAndGet();
            return entry;
        }
        try {
            Message request_message = Message.FETCH_ENTRY(clientId, key);
            if (lock != null) {
                if (!lock.getKey().equals(key)) {
                    LOGGER.log(Level.SEVERE, "lock {0} is not for key {1}", new Object[]{lock, key});
                    return null;
                }
                request_message.setParameter("lockId", lock.getLockId());
            }
            Message message = _channel.sendMessageWithReply(request_message, invalidateTimeout);
            LOGGER.log(Level.FINEST, "fetch result " + key + ", answer is " + message);
            if (internalClientListener != null) {
                internalClientListener.onFetchResponse(key, message);
            }
            if (message.type == Message.TYPE_ACK && runningFetches.consumeAndValidateFetchForKey(key, fetchId)) {
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                entry = new CacheEntry(key, System.nanoTime(), data, expiretime);
                storeEntry(entry);
                this.clientMissedGetsToSuccessfulFetches.incrementAndGet();
                this.clientHits.incrementAndGet();
                return entry;
            }
        } catch (TimeoutException err) {
            LOGGER.log(Level.SEVERE, "fetch failed " + key + ": " + err);
        }
        runningFetches.consumeAndValidateFetchForKey(key, fetchId);
        this.clientMissedGetsToMissedFetches.incrementAndGet();
        return null;

    }

    private void storeEntry(CacheEntry entry) {
        CacheEntry prev = cache.put(entry.getKey(), entry);
        if (prev != null) {
            actualMemory.addAndGet(-prev.getSerializedData().length);
        }
        actualMemory.addAndGet(entry.getSerializedData().length);
    }

    /**
     * Modifies the expireTime for a given entry. Expiration works at
     * CacheServer side.
     *
     * @param key
     * @param expiretime
     */
    public void touchEntry(String key, long expiretime) {
        touchEntry(key, expiretime, null);
    }

    /**
     * Modifies the expireTime for a given entry. Expiration works at
     * CacheServer side.
     *
     * @param key
     * @param expiretime
     * @see #lock(java.lang.String)
     */
    public void touchEntry(String key, long expiretime, KeyLock lock) {
        Channel _channel = channel;
        if (_channel != null) {
            Message request = Message.TOUCH_ENTRY(clientId, key, expiretime);
            if (lock != null) {
                if (!lock.getKey().equals(key)) {
                    return;
                }
                request.setParameter("lockId", lock.getLockId());
            }
            _channel.sendOneWayMessage(request, new SendResultCallback() {
                @Override
                public void messageSent(Message originalMessage, Throwable error) {
                    if (error != null) {
                        LOGGER.log(Level.SEVERE, "touch " + key + " failed ", error);
                    } else {
                        LOGGER.log(Level.FINEST, "touch " + key);
                        clientTouches.incrementAndGet();
                    }
                }
            });
        }
    }

    /**
     * Returns an entry from the local cache. No network operations will be
     * executed
     *
     * @param key
     * @return
     * @see #fetch(java.lang.String)
     */
    public CacheEntry get(String key) {
        if (channel == null) {
            LOGGER.log(Level.SEVERE, "get failed " + key + ", not connected");
            return null;
        }
        CacheEntry entry = cache.get(key);
        this.clientGets.incrementAndGet();
        if (entry != null) {
            entry.setLastGetTime(System.nanoTime());
            this.clientHits.incrementAndGet();
            return entry;
        }
        return null;
    }

    private static final int invalidateTimeout = 240000;

    /**
     * Invalidates an entry from the local cache and blocks until any other
     * client which holds the same entry has invalidated the entry locally.
     *
     * @param key
     * @throws InterruptedException
     */
    public void invalidate(String key) throws InterruptedException {
        invalidate(key, null);
    }

    public void invalidate(String key, KeyLock lock) throws InterruptedException {
        if (lock != null) {
            if (!lock.getKey().equals(key)) {
                return;
            }
        }

        // subito rimuoviamo dal locale
        CacheEntry removed = cache.remove(key);
        if (removed != null) {
            actualMemory.addAndGet(-removed.getSerializedData().length);
        }

        while (!stopped) {
            Channel _channel = channel;
            if (_channel == null) {
                LOGGER.log(Level.SEVERE, "invalidate " + key + ", not connected");
                Thread.sleep(1000);
                // if we are disconnected no lock can be valid
                lock = null;
            } else {
                try {
                    Message request = Message.INVALIDATE(clientId, key);
                    if (lock != null) {
                        request.setParameter("lockId", lock.getLockId());
                    }
                    Message response = _channel.sendMessageWithReply(request, invalidateTimeout);
                    LOGGER.log(Level.FINEST, "invalidate " + key + ", -> " + response);
                    this.clientInvalidations.incrementAndGet();
                    return;
                } catch (TimeoutException error) {
                    LOGGER.log(Level.SEVERE, "invalidate " + key + ", timeout " + error);
                    Thread.sleep(1000);
                }
            }
        }

    }

    /**
     * Same as {@link #invalidate(java.lang.String) } but it applies to every
     * entry whose key 'startsWith' the given prefix.
     *
     * @param prefix
     * @throws InterruptedException
     */
    public void invalidateByPrefix(String prefix) throws InterruptedException {
        // subito rimuoviamo dal locale
        Collection<String> keys = cache.keySet().stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
        keys.forEach((key) -> {
            CacheEntry removed = cache.remove(key);
            if (removed != null) {
                actualMemory.addAndGet(-removed.getSerializedData().length);
            }
        });

        while (!stopped) {
            Channel _channel = channel;
            if (_channel == null) {
                LOGGER.log(Level.SEVERE, "invalidateByPrefix " + prefix + ", not connected");
                Thread.sleep(1000);
            } else {
                try {
                    Message response = _channel.sendMessageWithReply(Message.INVALIDATE_BY_PREFIX(clientId, prefix), invalidateTimeout);
                    LOGGER.log(Level.FINEST, "invalidateByPrefix " + prefix + ", -> " + response);
                    this.clientInvalidations.incrementAndGet();
                    return;
                } catch (TimeoutException error) {
                    LOGGER.log(Level.SEVERE, "invalidateByPrefix " + prefix + ", timeout " + error);
                    Thread.sleep(1000);
                }
            }
        }

    }

    /**
     * Put an entry on the local cache. This method will also notify of the
     * change to all other clients which hold the same entry locally.
     *
     * @param key
     * @param data
     * @param expireTime This is the UNIX timestamp at which the entry should be
     * invalidated automatically. Use 0 in order to create an immortal entry
     * @return
     * @throws InterruptedException
     * @throws CacheException
     * @see #touchEntry(java.lang.String, long)
     */
    public boolean put(String key, byte[] data, long expireTime) throws InterruptedException, CacheException {
        return put(key, data, expireTime, null);
    }

    public boolean put(String key, byte[] data, long expireTime, KeyLock lock) throws InterruptedException, CacheException {
        Channel _chanel = channel;
        if (_chanel == null) {
            LOGGER.log(Level.SEVERE, "cache put failed " + key + ", not connected");
            return false;
        }
        if (lock != null && !lock.getKey().equals(key)) {
            throw new CacheException("lock " + lock + " is not for key " + key);
        }

        try {
            CacheEntry entry = new CacheEntry(key, System.nanoTime(), data, expireTime);
            CacheEntry prev = cache.put(key, entry);
            if (prev != null) {
                actualMemory.addAndGet(-prev.getSerializedData().length);
            }
            actualMemory.addAndGet(data.length);
            Message request = Message.PUT_ENTRY(clientId, key, data, expireTime);
            if (lock != null) {
                request.setParameter("lockId", lock.getLockId());
            }
            Message response = _chanel.sendMessageWithReply(request, invalidateTimeout);
            if (response.type != Message.TYPE_ACK) {
                throw new CacheException("error while putting key " + key + " (" + response + ")");
            }
            // race condition: if two clients perform a put on the same entry maybe after the network trip we get another value, different from the expected one.
            // it is better to invalidate the entry for all
            CacheEntry afterNetwork = cache.get(key);
            if (afterNetwork != null) {
                if (!Arrays.equals(afterNetwork.getSerializedData(), data)) {
                    LOGGER.log(Level.SEVERE, "detected conflict on put of " + key + ", invalidating entry");
                    invalidate(key);
                }
            }
            this.clientPuts.incrementAndGet();
            return true;
        } catch (TimeoutException timedOut) {
            throw new CacheException("error while putting for key " + key + ":" + timedOut, timedOut);
        }

    }

    public KeyLock lock(String key) throws InterruptedException, CacheException {
        Channel _chanel = channel;
        if (_chanel == null) {
            LOGGER.log(Level.SEVERE, "cache lock failed " + key + ", not connected");
            return null;
        }
        try {
            Message response = _chanel.sendMessageWithReply(Message.LOCK(clientId, key), invalidateTimeout);
            if (response.type != Message.TYPE_ACK) {
                throw new CacheException("error while locking key " + key + " (" + response + ")");
            }
            String lockId = (String) response.parameters.get("lockId");
            KeyLock result = new KeyLock();
            result.setLockId(lockId);
            result.setKey(key);
            return result;
        } catch (TimeoutException timedOut) {
            throw new CacheException("error while locking key " + key + ":" + timedOut, timedOut);
        }
    }

    public void unlock(KeyLock keyLock) throws InterruptedException, CacheException {
        if (keyLock == null) {
            return;
        }
        Channel _chanel = channel;
        if (_chanel == null) {
            LOGGER.log(Level.SEVERE, "cache unlock failed " + keyLock + ", not connected. lock already got released at network failure");
            return;
        }
        try {
            Message response = _chanel.sendMessageWithReply(Message.UNLOCK(clientId, keyLock.getKey(), keyLock.getLockId()), invalidateTimeout);
            if (response.type != Message.TYPE_ACK) {
                throw new CacheException("error while unlocking key " + keyLock.getKey() + " with lockID " + keyLock.getLockId() + " (" + response + ")");
            }
        } catch (TimeoutException timedOut) {
            throw new CacheException("error while unlockingkey " + keyLock.getKey() + " with lockID " + keyLock.getLockId() + ":" + timedOut, timedOut);
        }
    }

    /**
     * Return the local key set
     *
     * @param prefix
     * @return
     */
    public Set<String> getLocalKeySetByPrefix(String prefix) {
        return cache.keySet().stream().filter(k -> k.startsWith(prefix)).collect(Collectors.toSet());
    }

    /**
     * Register the statistics mbean related to this client if the input param
     * is set to true.
     * <p>
     * If the param is false, the statistics mbean would not be enabled.
     *
     * @param enabled true in order to enable statistics publishing on JMX
     */
    public void enableJmx(final boolean enabled) {
        if (enabled) {
            blazingcache.management.JMXUtils.registerClientStatisticsMXBean(this, statisticsMXBean);
            blazingcache.management.JMXUtils.registerClientStatusMXBean(this, statusMXBean);
        } else {
            blazingcache.management.JMXUtils.unregisterClientStatisticsMXBean(this);
            blazingcache.management.JMXUtils.unregisterClientStatusMXBean(this);
        }
    }

    /**
     *
     * @return number of puts executed since client boot
     */
    public long getClientPuts() {
        return this.clientPuts.get();
    }

    /**
     *
     * @return number of touches executed since client boot
     */
    public long getClientTouches() {
        return this.clientTouches.get();
    }

    /**
     *
     * @return number of gets executed since client boot
     */
    public long getClientGets() {
        return this.clientGets.get();
    }

    /**
     *
     * @return number of fetches executed since client boot
     */
    public long getClientFetches() {
        return this.clientFetches.get();
    }

    /**
     *
     * @return number of evictions executed since client boot
     */
    public long getClientEvictions() {
        return this.clientEvictions.get();
    }

    /**
     *
     * @return number of invalidations executed since client boot
     */
    public long getClientInvalidations() {
        return this.clientInvalidations.get();
    }

    /**
     *
     * @return number of hits occurred since client boot
     */
    public long getClientHits() {
        return this.clientHits.get();
    }

    /**
     *
     * @return number of missed gets ending with a successful remote read.
     */
    public long getClientMissedGetsToSuccessfulFetches() {
        return this.clientMissedGetsToSuccessfulFetches.get();
    }

    /**
     *
     * @return number of missed gets that ended with an unsuccessful remote read
     * as well.
     */
    public long getClientMissedGetsToMissedFetches() {
        return this.clientMissedGetsToMissedFetches.get();
    }

    /**
     * Return actual statistics. Statistics are always computed even if not
     * enabled
     *
     * @return actual statistics
     */
    public CacheClientStatisticsMXBean getStatistics() {
        return statisticsMXBean;
    }

}
