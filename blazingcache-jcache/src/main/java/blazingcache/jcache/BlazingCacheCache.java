/*
 * Copyright 2015 enrico.olivelli.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package blazingcache.jcache;

import blazingcache.client.CacheClient;
import blazingcache.client.CacheEntry;
import blazingcache.client.CacheException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 * Implementation of JSR 107 CacheManager
 *
 * @author enrico.olivelli
 */
public class BlazingCacheCache<K, V> implements Cache<K, V> {

    private final String cacheName;
    private final CacheClient client;
    private final Serializer<Object, String> keysSerializer;
    private final Serializer<Object, byte[]> valuesSerializer;
    private final boolean usefetch;
    private final Configuration<K, V> configuration;
    private final CacheManager cacheManager;
    private volatile boolean closed;
    private final long defaultTtl;

    public BlazingCacheCache(String cacheName, CacheClient client, CacheManager cacheManager, Serializer<Object, String> keysSerializer, Serializer<Object, byte[]> valuesSerializer, boolean usefetch, Configuration<K, V> configuration) {
        this.cacheName = cacheName;
        this.cacheManager = cacheManager;
        this.client = client;
        this.configuration = configuration;
        this.keysSerializer = keysSerializer;
        this.valuesSerializer = valuesSerializer;
        this.usefetch = usefetch;
        if (configuration instanceof CompleteConfiguration) {
            CompleteConfiguration cc = (CompleteConfiguration) configuration;
            ExpiryPolicy policy = (ExpiryPolicy) cc.getExpiryPolicyFactory().create();
            if (policy.getExpiryForAccess().isEternal()) {
                defaultTtl = -1;
            } else if (policy.getExpiryForAccess().isZero()) {
                defaultTtl = 1;
            } else {
                defaultTtl = policy.getExpiryForAccess().getTimeUnit().convert(policy.getExpiryForAccess().getDurationAmount(), TimeUnit.MILLISECONDS);
            }
        } else {
            defaultTtl = -1;
        }
    }

    public V getNoFetch(K key) {
        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);

        CacheEntry result = client.get(serializedKey);
        if (result != null) {
            return (V) valuesSerializer.deserialize(result.getSerializedData());
        } else {
            return null;
        }
    }

    @Override
    public V get(K key) {
        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
        System.out.println("get: "+client.getStatus());
        try {
            CacheEntry result;
            if (usefetch) {
                result = client.fetch(serializedKey);
            } else {
                result = client.get(serializedKey);
            }
            if (result != null) {
                return (V) valuesSerializer.deserialize(result.getSerializedData());
            } else {
                return null;
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }

    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        Map<K, V> result = new HashMap<>();
        for (K key : keys) {
            V r = get(key);
            if (r != null) {
                result.put(key, r);
            }
        }
        return result;
    }

    @Override
    public boolean containsKey(K key) {
        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
        return client.get(serializedKey) != null;
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void put(K key, V value) {
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
            return actual;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean remove(K key) {
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.invalidate(serializedKey);
            return false;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            if (Objects.equals(actual, oldValue)) {
                client.invalidate(serializedKey);
                return true;
            }
            return false;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public V getAndRemove(K key) {
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            client.invalidate(serializedKey);
            return actual;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean replace(K key, V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public V getAndReplace(K key, V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        for (K key : keys) {
            remove(key);
        }
    }

    @Override
    public void removeAll() {
        try {
            client.invalidateByPrefix(cacheName + "#");
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public void clear() {
        try {
            client.invalidateByPrefix(cacheName + "#");
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        return null;
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
