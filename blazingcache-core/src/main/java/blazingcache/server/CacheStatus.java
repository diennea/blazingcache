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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Gestione listeners
 *
 * @author enrico.olivelli
 */
public class CacheStatus {

    private static final Logger LOGGER = Logger.getLogger(CacheStatus.class.getName());

    private final Map<String, Set<String>> clientsForKey = new HashMap<>();
    private final Map<String, Set<String>> keysForClient = new HashMap<>();
    private final Map<String, Long> entryExpireTime = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "CacheListeners{" + "clientsForKey=" + clientsForKey + ", keysForClient=" + keysForClient + '}';
        } finally {
            lock.readLock().unlock();
        }
    }

    public void registerKeyForClient(String key, String client, long expiretime) {
        LOGGER.severe("registerKeyForClient key=" + key + " client=" + client);
        lock.writeLock().lock();
        try {
            Set<String> clients = clientsForKey.get(key);
            if (clients == null) {
                clients = new HashSet<>();
                clientsForKey.put(key, clients);
            }
            clients.add(client);

            Set<String> keys = keysForClient.get(client);
            if (keys == null) {
                keys = new HashSet<>();
                keysForClient.put(client, keys);
            }
            keys.add(key);
            if (expiretime > 0) {
                entryExpireTime.put(key, expiretime);
            } else {
                entryExpireTime.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Set<String> getClientsForKey(String key) {
        lock.readLock().lock();
        try {
            Set<String> clients = clientsForKey.get(key);
            if (clients == null) {
                return Collections.emptySet();
            } else {
                return new HashSet<>(clients);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getTotalEntryCount() {
        lock.readLock().lock();
        try {
            return clientsForKey.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<String> getKeys() {
        lock.readLock().lock();
        try {
            return new HashSet<>(clientsForKey.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<String> getKeysForClient(String client) {
        lock.readLock().lock();
        try {
            Set<String> keys = keysForClient.get(client);
            if (keys == null) {
                return Collections.emptyList();
            } else {
                return new ArrayList<>(keys);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    public void removeKeyForClient(String key, String client) {
        LOGGER.severe("removeKeyForClient key=" + key + " client=" + client);
        lock.writeLock().lock();
        try {
            Set<String> clients = clientsForKey.get(key);
            if (clients != null) {
                clients.remove(client);
                if (clients.isEmpty()) {
                    clientsForKey.remove(key);
                    entryExpireTime.remove(key);
                }
            }

            Set<String> keys = keysForClient.get(client);
            if (keys != null) {
                keys.remove(key);
                if (keys.isEmpty()) {
                    keysForClient.remove(client);
                }
            }
            LOGGER.log(Level.FINEST, "removeKeyForClient key={0} client={1} -> keysForClient {2}", new Object[]{key, client, keysForClient});
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeKeyByPrefixForClient(String prefix, String client) {
        LOGGER.severe("removeKeyByPrefixForClient prefix=" + prefix + " client=" + client);
        lock.writeLock().lock();
        try {

            Set<String> keys = keysForClient.get(client);
            Set<String> selectedKeys;
            if (keys != null) {
                selectedKeys = keys.stream().filter(key -> key.startsWith(prefix)).collect(Collectors.toSet());
                keys.removeAll(selectedKeys);
                if (keys.isEmpty()) {
                    keysForClient.remove(client);
                }
            } else {
                selectedKeys = Collections.emptySet();
            }
            for (String key : selectedKeys) {
                Set<String> clients = clientsForKey.get(key);
                if (clients != null) {
                    clients.remove(client);
                    if (clients.isEmpty()) {
                        clientsForKey.remove(key);
                        entryExpireTime.remove(key);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    int removeClientListeners(String client) {
        AtomicInteger count = new AtomicInteger();
        lock.writeLock().lock();
        try {
            Set<String> keys = keysForClient.get(client);
            if (keys != null) {
                keys.forEach((key) -> {
                    count.incrementAndGet();
                    Set<String> clients = clientsForKey.get(key);
                    if (clients != null) {
                        clients.remove(client);
                        if (clients.isEmpty()) {
                            clientsForKey.remove(key);
                            entryExpireTime.remove(key);
                        }
                    }
                });
            }
            keysForClient.remove(client);
        } finally {
            lock.writeLock().unlock();
        }
        return count.intValue();
    }

    Set<String> getAllClientsWithListener() {
        lock.readLock().lock();
        try {
            Set<String> clients = keysForClient.keySet();
            return new HashSet<>(clients);
        } finally {
            lock.readLock().unlock();
        }
    }

    List<String> selectExpiredEntries(long now, int max) {
        lock.readLock().lock();
        try {
            return entryExpireTime.entrySet().stream().filter(entry -> entry.getValue() < now).peek(
                    (entry) -> {
                        System.out.println("Entry " + entry.getKey() + ", expire: " + new java.sql.Timestamp(entry.getValue()) + " expired!");
                    }).map(entry -> entry.getKey()).limit(max).collect(Collectors.toList());
        } finally {
            lock.readLock().unlock();
        }
    }
}
