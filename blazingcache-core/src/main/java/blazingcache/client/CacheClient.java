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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
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

/**
 * Client
 *
 * @author enrico.olivelli
 */
public class CacheClient implements ChannelEventListener, ConnectionRequestInfo, AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(CacheClient.class.getName());

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final ServerLocator brokerLocator;
    private final Thread coreThread;
    private final String clientId;
    private final String sharedSecret;
    private volatile boolean stopped = false;
    private Channel channel;
    private long connectionTimestamp;

    /**
     * Maximum amount of memory used for storing entry values. 0 or negative to
     * disable
     */
    private long maxMemory = 0;

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
    }

    private final AtomicLong actualMemory = new AtomicLong();

    public long getActualMemory() {
        return actualMemory.get();
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
    }

    public ServerLocator getBrokerLocator() {
        return brokerLocator;
    }

    public void start() {
        this.coreThread.start();
    }

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

    public boolean isConnected() {
        return channel != null;
    }

    public long getConnectionTimestamp() {
        return connectionTimestamp;
    }

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
    }

    public void disconnect() {
        try {
            this.cache.clear();
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

    private static final Logger CONNECTION_MANAGER_LOGGER = Logger.getLogger(CacheClient.ConnectionManager.class.getName());

    private final class ConnectionManager implements Runnable {

        @Override
        public void run() {
            while (!stopped) {
                try {
                    if (channel == null || !channel.isValid()) {
                        connect();
                    }

                } catch (InterruptedException exit) {
                    CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "interrupted loop " + exit, exit);
                    continue;
                } catch (ServerNotAvailableException | ServerRejectedConnectionException retry) {
                    CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "no broker available:" + retry);
                }

                if (channel == null) {
                    try {
                        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "not connected, waiting 5000 ms");
                        Thread.sleep(5000);
                    } catch (InterruptedException exit) {
                        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "interrupted loop " + exit, exit);
                    }
                    continue;
                }
                if (maxMemory > 0) {
                    try {
                        ensureMaxMemoryLimit();
                    } catch (InterruptedException exit) {
                        CONNECTION_MANAGER_LOGGER.log(Level.SEVERE, "interrupted loop " + exit, exit);
                        continue;
                    }
                }
                try {
                    // TODO: wait for IO error or stop condition before reconnect 
                    CONNECTION_MANAGER_LOGGER.log(Level.FINEST, "connected");
                    Thread.sleep(5000);
                } catch (InterruptedException exit) {
                    LOGGER.log(Level.SEVERE, "interrupted loop " + exit, exit);
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
        LOGGER.severe("trying to release " + to_release + " bytes");
        List<CacheEntry> evictable = new ArrayList<>();
        java.util.function.Consumer<CacheEntry> accumulator = new java.util.function.Consumer<CacheEntry>() {
            long releasedMemory = 0;

            @Override
            public void accept(CacheEntry t) {
                if (releasedMemory < to_release) {
                    LOGGER.severe("evaluating " + t.getKey() + " " + t.getLastGetTime() + " size " + t.getSerializedData().length);
                    evictable.add(t);
                    releasedMemory += t.getSerializedData().length;
                }
            }
        };
        cache.values().stream().sorted(
                new Comparator<CacheEntry>() {

            @Override
            public int compare(CacheEntry o1, CacheEntry o2) {
                long diff = o1.lastGetTime - o2.lastGetTime;
                if (diff == 0) {
                    return 0;
                }
                return diff > 0 ? 1 : -1;
            }
        }
        ).forEachOrdered(accumulator);
        LOGGER.severe("found " + evictable.size() + " evictable entries");

        CountDownLatch count = new CountDownLatch(evictable.size());
        for (CacheEntry entry : evictable) {
            LOGGER.severe("evict " + entry.getKey() + " size " + entry.getSerializedData().length + " bytes lastAccessDate " + entry.getLastGetTime());
            CacheEntry removed = cache.remove(entry.getKey());
            if (removed != null) {
                actualMemory.addAndGet(-removed.getSerializedData().length);
                Channel _channel = channel;
                if (_channel != null) {
                    _channel.sendMessageWithAsyncReply(Message.UNREGISTER_ENTRY(clientId, entry.getKey()), new ReplyCallback() {

                        @Override
                        public void replyReceived(Message originalMessage, Message message, Throwable error) {
                            count.countDown();
                        }
                    });
                } else {
                    count.countDown();
                }
            }
        }
        count.await();
    }

    @Override
    public void messageReceived(Message message) {
        LOGGER.log(Level.FINER, "{0} messageReceived {1}", new Object[]{clientId, message});
        switch (message.type) {
            case Message.TYPE_INVALIDATE: {
                String key = (String) message.parameters.get("key");
                LOGGER.log(Level.SEVERE, clientId + " invalidate " + key + " from " + message.clientId);
                CacheEntry removed = cache.remove(key);
                Channel _channel = channel;
                if (_channel != null) {
                    _channel.sendReplyMessage(message, Message.ACK(clientId));
                }
                if (removed != null) {
                    actualMemory.addAndGet(-removed.getSerializedData().length);
                }
            }
            break;
            case Message.TYPE_INVALIDATE_BY_PREFIX: {
                String prefix = (String) message.parameters.get("prefix");
                LOGGER.log(Level.SEVERE, clientId + " invalidateByPrefix " + prefix + " from " + message.clientId);
                Collection<String> keys = cache.keySet().stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
                keys.forEach((key) -> {
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
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                LOGGER.log(Level.SEVERE, clientId + " put " + key + " from " + message.clientId);
                cache.put(key, new CacheEntry(key, System.nanoTime(), expiretime, data, expiretime));
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
                LOGGER.log(Level.SEVERE, clientId + " fetch " + key + " from " + message.clientId + " -> " + entry);
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

    @Override
    public void close() throws Exception {
        stop();
    }

    public void stop() {
        LOGGER.log(Level.SEVERE, "stopping", new Exception("stopping").fillInStackTrace());
        stopped = true;
        try {
            coreThread.interrupt();
            coreThread.join();
        } catch (InterruptedException ex) {
            LOGGER.log(Level.SEVERE, "stop interrupted", ex);
        }
    }

    public CacheEntry fetch(String key) throws InterruptedException {
        Channel _channel = channel;
        if (_channel == null) {
            LOGGER.log(Level.SEVERE, "fetch failed " + key + ", not connected");
            return null;
        }
        CacheEntry entry = cache.get(key);
        if (entry != null) {
            entry.lastGetTime = System.nanoTime();
            return entry;
        }
        try {
            Message message = _channel.sendMessageWithReply(Message.FETCH_ENTRY(clientId, key), invalidateTimeout);
            LOGGER.log(Level.FINEST, "fetch result " + key + ", answer is " + message);
            if (message.type == Message.TYPE_ACK) {
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                entry = new CacheEntry(key, System.currentTimeMillis(), expiretime, data, expiretime);
                cache.put(key, entry);
                return entry;
            } else {
                return null;
            }
        } catch (TimeoutException err) {
            LOGGER.log(Level.SEVERE, "get failed " + key + ": " + err);
            return null;
        }

    }

    public CacheEntry get(String key) {
        if (channel == null) {
            LOGGER.log(Level.SEVERE, "get failed " + key + ", not connected");
            return null;
        }
        CacheEntry entry = cache.get(key);
        if (entry != null) {
            entry.lastGetTime = System.nanoTime();
        }
        return entry;
    }

    private static final int invalidateTimeout = 240000;

    public void invalidate(String key) throws InterruptedException {
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
            } else {
                try {
                    Message response = _channel.sendMessageWithReply(Message.INVALIDATE(clientId, key), invalidateTimeout);
                    LOGGER.log(Level.SEVERE, "invalidate " + key + ", -> " + response);
                    return;
                } catch (TimeoutException error) {
                    LOGGER.log(Level.SEVERE, "invalidate " + key + ", timeout " + error);
                    Thread.sleep(1000);
                }
            }
        }

    }

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
                    LOGGER.log(Level.SEVERE, "invalidateByPrefix " + prefix + ", -> " + response);
                    return;
                } catch (TimeoutException error) {
                    LOGGER.log(Level.SEVERE, "invalidateByPrefix " + prefix + ", timeout " + error);
                    Thread.sleep(1000);
                }
            }
        }

    }

    public boolean put(String key, byte[] data, long expireTime) throws InterruptedException, CacheException {
        Channel _chanel = channel;
        if (_chanel == null) {
            LOGGER.log(Level.SEVERE, "cache put failed " + key + ", not connected");
            return false;
        }
        try {
            CacheEntry entry = new CacheEntry(key, System.nanoTime(), expireTime, data, expireTime);
            CacheEntry prev = cache.put(key, entry);
            Message response = _chanel.sendMessageWithReply(Message.PUT_ENTRY(clientId, key, data, expireTime), invalidateTimeout);
            if (response.type != Message.TYPE_ACK) {
                throw new CacheException("error while putting key " + key + " (" + response + ")");
            }
            // race condition: if two clients perform a put on the same entry maybe after the network trip we get another value, different from the expected one.
            // it is better to invalidate the entry for alll
            CacheEntry afterNetwork = cache.get(key);
            if (afterNetwork != null) {
                if (Arrays.equals(afterNetwork.getSerializedData(), data)) {
                    if (prev != null) {
                        actualMemory.addAndGet(-prev.getSerializedData().length);
                    }
                    actualMemory.addAndGet(data.length);
                } else {
                    LOGGER.log(Level.SEVERE, "detected conflict on put of " + key + ", invalidating entry");
                    invalidate(key);
                }
            }
            return true;
        } catch (TimeoutException timedOut) {
            throw new CacheException("error while putting for key " + key + ":" + timedOut, timedOut);
        }

    }

}
