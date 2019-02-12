/*
 * Copyright 2015 Diennea S.R.L..
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
import blazingcache.client.EntryHandle;
import blazingcache.client.CacheException;
import blazingcache.client.KeyLock;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
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

    final static boolean JSR107_TCK_101_COMPAT_MODE = Boolean.getBoolean("org.blazingcache.jsr107tck101compatmode");

    private final String cacheName;
    private final CacheClient client;
    private final Serializer<K, String, String> keysSerializer;
    private final Serializer<V, InputStream, byte[]> valuesSerializer;
    private final boolean usefetch;
    private final MutableConfiguration<K, V> configuration;
    private final CacheManager cacheManager;
    private volatile boolean closed;
    private final boolean storeByReference;

    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;
    private final Class<V> valueType;
    private final Class<K> keyType;
    private final boolean isReadThrough;
    private final boolean isWriteThrough;
    private final ExpiryPolicy policy;
    private final BlazingCacheConfigurationMXBean<K, V> configurationMXBean;
    private final BlazingCacheStatisticsMXBean<K, V> statisticsMXBean;
    private boolean needPreviuosValueForListeners = false;
    private List<BlazingCacheCacheEntryListenerWrapper<K, V>> listeners = new ArrayList<>();

    public BlazingCacheCache(String cacheName, CacheClient client, CacheManager cacheManager, Serializer<K, String, String> keysSerializer,
            Serializer<V, InputStream, byte[]> valuesSerializer, boolean usefetch, Configuration<K, V> configuration) {
        this.cacheName = cacheName;
        this.cacheManager = cacheManager;
        this.client = client;
        this.keysSerializer = keysSerializer;
        this.valuesSerializer = valuesSerializer;
        this.valueType = configuration.getValueType();
        this.keyType = configuration.getKeyType();
        this.usefetch = usefetch;
        this.configurationMXBean = new BlazingCacheConfigurationMXBean<>(this);
        this.statisticsMXBean = new BlazingCacheStatisticsMXBean<>(this);
        this.storeByReference = !configuration.isStoreByValue();
        if (configuration instanceof CompleteConfiguration) {
            this.configuration = new MutableConfiguration<>((CompleteConfiguration<K, V>) configuration);
            CompleteConfiguration<K, V> cc = (CompleteConfiguration<K, V>) configuration;
            if (cc.getExpiryPolicyFactory() == null) {
                throw new IllegalArgumentException("ExpiryPolicyFactory cannot be null");
            } else {
                ExpiryPolicy _policy = cc.getExpiryPolicyFactory().create();;
                if (_policy == null) {
                    throw new IllegalArgumentException("ExpiryPolicy cannot be null");
                }
                if (_policy instanceof EternalExpiryPolicy) {
                    this.policy = null; // shortcut for the most common case
                } else {
                    this.policy = _policy;
                }
            }

            if (cc.getCacheLoaderFactory() != null) {
                cacheLoader = (CacheLoader) cc.getCacheLoaderFactory().create();
            } else {
                cacheLoader = null;
            }
            if (cc.getCacheWriterFactory() != null) {
                cacheWriter = (CacheWriter<K, V>) cc.getCacheWriterFactory().create();
            } else {
                cacheWriter = null;
            }
            isReadThrough = cc.isReadThrough();
            isWriteThrough = cc.isWriteThrough();
            needPreviuosValueForListeners = policy != null;
            if (cc.getCacheEntryListenerConfigurations() != null) {
                for (CacheEntryListenerConfiguration<K, V> listenerConfig : cc.getCacheEntryListenerConfigurations()) {
                    configureListener(listenerConfig);
                }
            }
        } else {
            this.configuration = new MutableConfiguration<K, V>()
                    .setTypes(configuration.getKeyType(), configuration.getValueType())
                    .setStoreByValue(configuration.isStoreByValue());
            this.policy = null; // means "eternal"
            cacheLoader = null;
            needPreviuosValueForListeners = false;
            cacheWriter = null;
            isReadThrough = false;
            isWriteThrough = false;
        }
        if (isReadThrough && cacheLoader == null) {
            throw new IllegalArgumentException("cache isReadThrough=" + isReadThrough + " cacheLoader=" + cacheLoader);
        }
        if (isWriteThrough && cacheWriter == null) {
            throw new IllegalArgumentException("cache isWriteThrough=" + isWriteThrough + " cacheWriter=" + cacheWriter);
        }
        if (this.configuration.isManagementEnabled()) {
            setManagementEnabled(true);
        }

        if (this.configuration.isStatisticsEnabled()) {
            setStatisticsEnabled(true);
        }

    }

    static String getCacheName(String key) {
        if (key == null) {
            return null;
        }
        int pos = key.indexOf('#');
        if (pos >= 0) {
            return key.substring(0, pos);
        } else {
            return null;
        }
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("this cache is closed");
        }
    }

    private V getNoFetch(String serializedKey) {
        if (storeByReference) {
            try {
                return (V) client.getObject(serializedKey);
            } catch (CacheException error) {
                throw new javax.cache.CacheException(error);
            }
        } else {
            EntryHandle result = client.get(serializedKey);
            if (result != null) {
                try {
                    return (V) valuesSerializer.deserialize(
                            result.getSerializedDataStream()
                    );
                } finally {
                    result.close();
                }
            } else {
                return null;
            }
        }
    }

    @Override
    public V get(K key) {
        checkClosed();
        if (key == null) {
            throw new NullPointerException();
        }
        return get(key, true, true, null);
    }

    private void fireEntryCreated(K key, V value) {
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listener : listeners) {
            try {
                listener.onEntryCreated(key, value);
            } catch (Exception err) {
            }
        }
    }

    private void fireEntryUpdated(K key, V prevValue, V value) {
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listener : listeners) {
            try {
                listener.onEntryUpdated(key, prevValue, value);
            } catch (Exception err) {
            }
        }
    }

    private void fireEntryRemoved(K key, V prevValue) {
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listener : listeners) {
            try {
                listener.onEntryRemoved(key, prevValue);
            } catch (Exception err) {
            }
        }
    }

    /**
     *
     * @param serializedKey
     * @return true is the entry still valid
     */
    private boolean handleEntryAccessed(String serializedKey, KeyLock lock) {
        long accessExpireTime = getAccessExpireTime();
        if (accessExpireTime < 0) {
            return true;
        } else if (accessExpireTime == 0) {
            return false;
        }
        client.touchEntry(serializedKey, nowPlusDuration(accessExpireTime), lock);
        return true;
    }

    private static long nowPlusDuration(long duration) {
        if (duration < 0) {
            return -1;
        }
        return System.currentTimeMillis() + duration;
    }

    private V get(K key, boolean allowLoader, boolean checkExpiryAccess, KeyLock lock) {
        checkClosed();
        String serializedKey = cacheNameAndKey(key);
        try {
            V resultObject = null;
            boolean hit = false;
            if (storeByReference) {
                if (usefetch) {
                    resultObject = client.fetchObject(serializedKey, lock);
                } else {
                    resultObject = client.getObject(serializedKey);
                }
                hit = resultObject != null;
            } else {
                EntryHandle result;
                if (usefetch) {
                    result = client.fetch(serializedKey, lock);
                } else {
                    result = client.get(serializedKey);
                }
                if (result != null) {
                    try {
                        hit = true;
                        resultObject = (V) valuesSerializer.deserialize(result.getSerializedDataStream());
                    } finally {
                        result.close();
                    }
                }
            }
            if (hit) {
                boolean validAfterAccess = !checkExpiryAccess || handleEntryAccessed(serializedKey, lock);
                if (!validAfterAccess) {
                    client.invalidate(serializedKey, lock);
                }
                cacheHits.incrementAndGet();
                return resultObject;
            }
            if (allowLoader && cacheLoader != null && isReadThrough) {
                V loaded;
                try {
                    loaded = cacheLoader.load(key);
                } catch (Exception err) {
                    throw new CacheLoaderException(err);
                }
                if (loaded != null) {
                    long createdExpireTime = getCreatedExpireTime();
                    if (createdExpireTime != 0) {
                        long expireTs = nowPlusDuration(createdExpireTime);
                        _put(serializedKey, loaded, expireTs, lock);
                    }
                    fireEntryCreated(key, loaded);
                    cacheHits.incrementAndGet();
                    return loaded;
                }
            }
            cacheMisses.incrementAndGet();
            return null;

        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }

    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return getAll(keys, true);
    }

    private Map<K, V> getAll(Set<? extends K> keys, boolean allowCheckAccess) {
        checkClosed();
        try {
            Map<K, V> map_result = new HashMap<>();
            Set<K> keysToLoad = new HashSet<>();
            for (K key : keys) {
                if (key == null) {
                    throw new NullPointerException();
                }
            }
            for (K key : keys) {

                String serializedKey = cacheNameAndKey(key);
                V r = null;
                boolean hit = false;
                V resultObject = null;
                if (storeByReference) {
                    if (usefetch) {
                        resultObject = client.fetchObject(serializedKey);
                    } else {
                        resultObject = client.getObject(serializedKey);
                    }
                    if (resultObject != null) {
                        hit = true;
                    }
                } else {
                    EntryHandle result;
                    if (usefetch) {
                        result = client.fetch(serializedKey);
                    } else {
                        result = client.get(serializedKey);
                    }
                    if (result != null) {
                        try {
                            hit = true;
                            resultObject = (V) valuesSerializer.deserialize(result.getSerializedDataStream());
                        } finally {
                            result.close();
                        }
                    }
                }
                if (hit) {
                    r = resultObject;
                } else if (cacheLoader != null && isReadThrough) {
                    keysToLoad.add(key);
                }

                if (r != null) {
                    boolean validAfterAccess = !allowCheckAccess || handleEntryAccessed(serializedKey, null);
                    if (!validAfterAccess) {
                        client.invalidate(serializedKey);
                    }
                    map_result.put(key, r);
                }
            }
            if (!keysToLoad.isEmpty()) {
                Map<K, V> loaded_all;
                try {
                    loaded_all = cacheLoader.loadAll(keysToLoad);
                } catch (Exception err) {
                    throw new CacheLoaderException(err);
                }
                for (Map.Entry<K, V> entry : loaded_all.entrySet()) {
                    K key = entry.getKey();
                    V loaded = entry.getValue();
                    if (loaded != null) {
                        String serializedKey = cacheNameAndKey(key);
                        long createdExpireTime = getCreatedExpireTime();
                        if (createdExpireTime != 0) {
                            long expireTs = nowPlusDuration(createdExpireTime);
                            _put(serializedKey, loaded, expireTs, null);
                        }

                        fireEntryCreated(key, loaded);
                        map_result.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            cacheHits.addAndGet(map_result.size());
            cacheMisses.addAndGet(keys.size() - map_result.size());
            return map_result;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }

    }

    @Override
    public boolean containsKey(K key) {
        checkClosed();
        if (key == null) {
            throw new NullPointerException();
        }
        String serializedKey = cacheNameAndKey(key);
        return _containsKey(serializedKey);
    }

    private boolean _containsKey(String serializedKey) {
        EntryHandle entry = client.get(serializedKey);
        if (entry != null) {
            entry.close();
            return true;
        } else {
            return false;
        }
    }

    private void _put(String serializedKey, V value, long expireTs, KeyLock lock) throws InterruptedException, CacheException {
        if (storeByReference) {
            client.putObject(serializedKey, value, expireTs, lock);
        } else {
            client.put(serializedKey, valuesSerializer.serialize(value), expireTs, lock);
        }
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        checkClosed();
        if (keys == null) {
            throw new NullPointerException();
        }

        Set<K> toLoad;
        if (replaceExistingValues) {
            toLoad = (Set<K>) keys;
            for (K k : keys) {
                if (k == null) {
                    throw new NullPointerException();
                }
            }
        } else {
            toLoad = new HashSet<>();
            for (K k : keys) {
                if (k == null) {
                    throw new NullPointerException();
                }
                if (!containsKey(k)) {
                    toLoad.add(k);
                }
            }
        }
        if (cacheLoader == null || keys.isEmpty()) {
            if (completionListener != null) {
                completionListener.onCompletion();
            }
            return;
        }
        try {
            if (!toLoad.isEmpty()) {
                Map<K, V> loaded;
                try {
                    loaded = cacheLoader.loadAll(toLoad);
                } catch (Exception err) {
                    throw new CacheLoaderException(err);
                }
                if (loaded != null) {
                    for (Map.Entry<K, V> loaded_entry : loaded.entrySet()) {
                        K key = loaded_entry.getKey();
                        V value = loaded_entry.getValue();
                        if (value != null) {
                            String serializedKey = cacheNameAndKey(key);
                            if (needPreviuosValueForListeners) {
                                V actual = getNoFetch(serializedKey);
                                if (actual == null) {
                                    long createdExpireTime = getCreatedExpireTime();
                                    if (createdExpireTime != 0) {
                                        long expireTs = nowPlusDuration(createdExpireTime);
                                        _put(serializedKey, value, expireTs, null);
                                        fireEntryCreated(key, value);
                                    }
                                } else {
                                    long updatedExpireTime = getUpdatedExpireTime();
                                    if (updatedExpireTime != 0) {
                                        long expireTs = nowPlusDuration(nowPlusDuration(updatedExpireTime));
                                        _put(serializedKey, value, expireTs, null);
                                        fireEntryUpdated(key, actual, value);
                                    } else {
                                        client.invalidate(serializedKey);
                                    }
                                }
                            } else {
                                long createdExpireTime = getCreatedExpireTime();
                                if (createdExpireTime != 0) {
                                    long expireTs = nowPlusDuration(createdExpireTime);
                                    _put(serializedKey, value, expireTs, null);
                                }
                            }
                        }
                    }
                }
            }
            if (completionListener != null) {
                completionListener.onCompletion();
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            if (completionListener != null) {
                completionListener.onException(err);
            }
        } catch (Exception ex) {
            if (completionListener != null) {
                completionListener.onException(ex);
            }
        }
    }

    private String cacheNameAndKey(K key) {
        String serializeKey = keysSerializer.serialize(key);
        return cacheName + "#" + serializeKey;
    }

    private void runtimeCheckType(K key, V value) {
        if (key != null && keyType != null && !keyType.isInstance(key)) {
            throw new ClassCastException(key.getClass().toString());
        }
        if (value != null && valueType != null && !valueType.isInstance(value)) {
            throw new ClassCastException(key.getClass().toString());
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    private void put(K key, V value, KeyLock lock) {
        checkClosed();

        if (key == null || value == null) {
            throw new NullPointerException();
        }

        runtimeCheckType(key, value);

        if (needPreviuosValueForListeners) {
            getAndPut(key, value, lock);
            return;
        }
        try {
            if (isWriteThrough) {
                try {
                    cacheWriter.write(new BlazingCacheEntry<>(key, value));
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            String serializedKey = cacheNameAndKey(key);
            long updatedExpireTime = getUpdatedExpireTime();
            if (updatedExpireTime != 0) {
                long expireTs = nowPlusDuration(updatedExpireTime);
                _put(serializedKey, value, expireTs, lock);
            } else {
                client.invalidate(serializedKey, lock);
            }
            cachePuts.incrementAndGet();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
        return getAndPut(key, value, null);
    }

    private V getAndPut(K key, V value, KeyLock lock) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        runtimeCheckType(key, value);
        try {

            if (isWriteThrough) {
                try {
                    cacheWriter.write(new BlazingCacheEntry<>(key, value));
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            String serializedKey = cacheNameAndKey(key);
            V actual = getNoFetch(serializedKey);
            if (actual == null) {
                cacheMisses.incrementAndGet();
                long createdExpireTime = getCreatedExpireTime();
                if (createdExpireTime != 0) {
                    long expireTs = nowPlusDuration(createdExpireTime);
                    _put(serializedKey, value, expireTs, lock);
                    cachePuts.incrementAndGet();
                    fireEntryCreated(key, value);
                }
            } else {
                cacheHits.incrementAndGet();
                long updatedExpireTime = getUpdatedExpireTime();
                if (updatedExpireTime != 0) {
                    long expireTs = nowPlusDuration(updatedExpireTime);
                    _put(serializedKey, value, expireTs, lock);
                    cachePuts.incrementAndGet();
                    fireEntryUpdated(key, actual, value);
                } else {
                    client.invalidate(serializedKey, lock);
                }
            }
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
        checkClosed();
        CacheWriterException cacheWriterError = null;
        try {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (key == null || value == null) {
                    throw new NullPointerException();
                }
            }
            map = new HashMap<>(map);// cloning
            if (isWriteThrough) {
                ArrayList<Cache.Entry<? extends K, ? extends V>> entries = new ArrayList<>();
                for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                    K key = entry.getKey();
                    V value = entry.getValue();
                    entries.add(new BlazingCacheEntry<>(key, value));
                }

                try {
                    if (!entries.isEmpty()) {
                        cacheWriter.writeAll(entries);
                    }
                } catch (Exception err) {
                    cacheWriterError = new CacheWriterException(err);
                }
                for (Cache.Entry<? extends K, ? extends V> entry : entries) {
                    // in case of partial success the entries collection will contain the un-written entries                                                            
                    map.remove(entry.getKey());
                }

            }
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                runtimeCheckType(key, value);
                if (needPreviuosValueForListeners) {
                    String serializedKey = cacheNameAndKey(key);
                    V previousForListener = getNoFetch(serializedKey);

                    if (previousForListener == null) {
                        long createdExpireTime = getCreatedExpireTime();
                        if (createdExpireTime != 0) {
                            long expireTs = nowPlusDuration(createdExpireTime);
                            _put(serializedKey, value, expireTs, null);
                            fireEntryCreated(key, value);
                            cachePuts.incrementAndGet();
                        }
                    } else {
                        long updatedExpireTime = getUpdatedExpireTime();
                        if (updatedExpireTime != 0) {
                            long expireTs = nowPlusDuration(updatedExpireTime);
                            _put(serializedKey, value, expireTs, null);
                            fireEntryUpdated(key, previousForListener, value);
                            cachePuts.incrementAndGet();
                        } else {
                            client.invalidate(serializedKey);
                        }
                    }
                } else {
                    long createdExpireTime = getCreatedExpireTime();
                    if (createdExpireTime != 0) {
                        String serializedKey = cacheNameAndKey(key);
                        long expireTs = nowPlusDuration(createdExpireTime);
                        _put(serializedKey, value, expireTs, null);
                        cachePuts.incrementAndGet();
                    }
                }

            }
            if (cacheWriterError != null) {
                throw cacheWriterError;
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        runtimeCheckType(key, value);
        if (!containsKey(key)) {
            if (!JSR107_TCK_101_COMPAT_MODE) {
                cacheMisses.incrementAndGet();
            }
            put(key, value);
            return true;
        } else {
            if (!JSR107_TCK_101_COMPAT_MODE) {
                cacheHits.incrementAndGet();
            }
            return false;
        }
    }

    @Override
    public boolean remove(K key) {
        checkClosed();
        return remove(key, true, null);
    }

    private boolean remove(K key, boolean allowWriteThrough, KeyLock lock) {
        if (key == null) {
            throw new NullPointerException();
        }
        try {
            String serializedKey = cacheNameAndKey(key);
            V actual = getNoFetch(serializedKey);
            if (allowWriteThrough && isWriteThrough) {
                try {
                    // write even if value is not cached locally
                    cacheWriter.delete(key);
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            if (actual == null) {
                return false;
            }
            client.invalidate(serializedKey, lock);
            fireEntryRemoved(key, actual);
            cacheRemovals.incrementAndGet();
            return true;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public boolean remove(K key, V oldValue) {
        checkClosed();
        if (key == null || oldValue == null) {
            throw new NullPointerException();
        }
        try {
            String serializedKey = cacheNameAndKey(key);
            KeyLock lock = client.lock(serializedKey);
            if (lock == null) {
                throw new CacheException("could not lock key " + key);
            }
            try {
                V actual = getNoFetch(serializedKey);
                if (actual == null) {
                    cacheMisses.incrementAndGet();
                    return false;
                }
                if (Objects.equals(actual, oldValue)) {
                    if (isWriteThrough) {
                        try {
                            cacheWriter.delete(key);
                        } catch (Exception err) {
                            throw new CacheWriterException(err);
                        }
                    }
                    client.invalidate(serializedKey, lock);

                    fireEntryRemoved(key, actual);
                    cacheHits.incrementAndGet();
                    cacheRemovals.incrementAndGet();
                    return true;
                }
                boolean validAfterAccess = handleEntryAccessed(serializedKey, lock);
                if (!validAfterAccess) {
                    client.invalidate(serializedKey, lock);
                }
                return false;
            } finally {
                client.unlock(lock);
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public V getAndRemove(K key) {
        checkClosed();
        if (key == null) {
            throw new NullPointerException();
        }
        try {
            if (isWriteThrough) {
                try {
                    cacheWriter.delete(key);
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            String serializedKey = cacheNameAndKey(key);
            V actual = getNoFetch(serializedKey);
            client.invalidate(serializedKey);
            if (actual != null) {
                cacheHits.incrementAndGet();
                cacheRemovals.incrementAndGet();
                fireEntryRemoved(key, actual);
            } else {
                cacheMisses.incrementAndGet();
            }
            return actual;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        checkClosed();
        if (key == null || oldValue == null || newValue == null) {
            throw new NullPointerException();
        }
        String serializedKey = cacheNameAndKey(key);

        if (_containsKey(serializedKey)) {
            try {
                KeyLock lock = client.lock(serializedKey);
                if (lock == null) {
                    return false;
                }
                try {
                    V actual = get(key, false, false, lock);
                    if (Objects.equals(actual, oldValue)) {
                        put(key, newValue, lock);
                        return true;
                    } else {
                        boolean stillValid = handleEntryAccessed(serializedKey, lock);
                        if (!stillValid) {
                            try {
                                client.invalidate(serializedKey);
                            } catch (InterruptedException err) {
                                Thread.currentThread().interrupt();
                                throw new javax.cache.CacheException(err);
                            }
                        }
                        return false;
                    }
                } finally {
                    client.unlock(lock);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new javax.cache.CacheException(interrupted);
            } catch (CacheException error) {
                throw new javax.cache.CacheException(error);
            }

        } else {
            cacheMisses.incrementAndGet();
            return false;
        }
    }

    @Override
    public boolean replace(K key, V value) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        String serializedKey = cacheNameAndKey(key);
        if (_containsKey(serializedKey)) {
            try {
                KeyLock lock = client.lock(serializedKey);
                if (lock == null) {
                    return false;
                }
                try {
                    cacheHits.incrementAndGet();
                    put(key, value, lock);
                    return true;
                } finally {
                    client.unlock(lock);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new javax.cache.CacheException(interrupted);
            } catch (CacheException error) {
                throw new javax.cache.CacheException(error);
            }
        } else {
            cacheMisses.incrementAndGet();
            return false;
        }
    }

    @Override
    public V getAndReplace(K key, V value) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        String serializedKey = cacheNameAndKey(key);
        if (_containsKey(serializedKey)) {
            try {
                KeyLock lock = client.lock(serializedKey);
                if (lock == null) {
                    throw new javax.cache.CacheException("could not grant lock");
                }
                try {
                    V oldValue = get(key, false, false, lock);
                    put(key, value, lock);
                    return oldValue;
                } finally {
                    client.unlock(lock);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new javax.cache.CacheException(interrupted);
            } catch (CacheException error) {
                throw new javax.cache.CacheException(error);
            }
        } else {
            cacheMisses.incrementAndGet();
            return null;
        }
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        checkClosed();
        Collection<K> notWrittenByWriter = null;
        CacheWriterException error = null;
        if (isWriteThrough && !keys.isEmpty()) {
            try {
                notWrittenByWriter = new ArrayList<>(keys);
                // the writer will leave entries which coult not be deleted from storage in the passed argument
                if (!notWrittenByWriter.isEmpty()) {
                    cacheWriter.deleteAll(notWrittenByWriter);
                }
            } catch (Exception err) {
                error = new CacheWriterException(err);
            }
        }
        if (notWrittenByWriter != null) {
            Collection<K> toRemove = new ArrayList<>(keys);
            toRemove.removeAll(notWrittenByWriter);
            for (K key : toRemove) {
                remove(key, false, null);
            }
        } else {
            for (K key : keys) {
                remove(key, false, null);
            }
        }
        if (error != null) {
            throw error;
        }

    }

    @Override
    public void removeAll() {
        checkClosed();
        try {
            Set<K> localKeys;
            Map<K, V> previousValuesForListener = null;
            if (isWriteThrough || needPreviuosValueForListeners || configuration.isStatisticsEnabled()) {
                int prefixLen = (cacheName + "#").length();
                localKeys = client
                        .getLocalKeySetByPrefix(cacheName + "#")
                        .stream()
                        .map(s -> {
                            return (K) keysSerializer.deserialize(s.substring(prefixLen));
                        }).collect(Collectors.toSet());
                if (needPreviuosValueForListeners) {
                    previousValuesForListener = new HashMap<>();
                    for (K key : localKeys) {
                        String serializedKey = cacheNameAndKey(key);
                        V value = getNoFetch(serializedKey);
                        previousValuesForListener.put(key, value);
                    }
                }
            } else {
                localKeys = null;
            }
            CacheWriterException cacheWriterError = null;
            Collection<K> unprocessedByWriter = null;
            if (localKeys != null && isWriteThrough) {
                unprocessedByWriter = new ArrayList<>(localKeys);
                try {
                    // the writer will leave entries which coult not be deleted from storage in the passed argument
                    if (!unprocessedByWriter.isEmpty()) {
                        cacheWriter.deleteAll(unprocessedByWriter);
                    }
                } catch (Exception err) {
                    cacheWriterError = new CacheWriterException(err);
                }
            }
            if (cacheWriterError == null) {
                client.invalidateByPrefix(cacheName + "#");
                if (previousValuesForListener != null) {
                    for (Map.Entry<K, V> entry : previousValuesForListener.entrySet()) {
                        fireEntryRemoved(entry.getKey(), entry.getValue());
                    }
                }
                if (localKeys != null) {
                    Collection<K> toRemove = new ArrayList<>(localKeys);
                    if (unprocessedByWriter != null) {
                        toRemove.removeAll(unprocessedByWriter);
                    }
                    cacheRemovals.addAndGet(toRemove.size());
                }
            } else {
                Collection<K> toRemove = new ArrayList<>(localKeys);
                toRemove.removeAll(unprocessedByWriter);
                for (K k : toRemove) {
                    remove(k, false, null);
                }
                throw cacheWriterError;
            }

        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public void clear() {
        checkClosed();
        try {
            client.invalidateByPrefix(cacheName + "#");
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public <C extends Configuration<K, V>>
            C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        checkClosed();
        if (key == null || entryProcessor == null) {
            throw new NullPointerException();
        }
        String serializedKey = cacheNameAndKey(key);

        try {
            KeyLock lock = client.lock(serializedKey);
            if (lock == null) {
                throw new javax.cache.CacheException("could not grant lock");
            }
            try {
                boolean present = _containsKey(serializedKey);
                V valueBeforeProcessor = get(key, true, false, lock);
                BlazingCacheCacheMutableEntry<K, V> entry = new BlazingCacheCacheMutableEntry<>(present, key, valueBeforeProcessor);
                T returnValue = entryProcessor.process(entry, arguments);
                boolean validAfterAccess = true;
                if (entry.isAccessed()) {
                    validAfterAccess = handleEntryAccessed(serializedKey, lock);
                }
                if (entry.isRemoved()) {
                    if (present) {
                        remove(key, true, lock);
                    } else if (isWriteThrough) {
                        try {
                            cacheWriter.delete(key);
                        } catch (Exception err) {
                            throw new CacheWriterException(err);
                        }
                    }
                } else if (entry.isUpdated()) {
                    put(key, entry.getValue(), lock);
                } else if (!validAfterAccess) {
                    client.invalidate(serializedKey, lock);
                }
                return returnValue;
            } finally {
                client.unlock(lock);
            }
        } catch (javax.cache.CacheException err) {
            throw err;
        } catch (Exception err) {
            throw new EntryProcessorException(err);
        }
    }

    private long getCreatedExpireTime() {
        if (policy == null) {
            return -1;
        }
        Duration res = policy.getExpiryForCreation();
        if (res == null || res.isEternal()) {
            return -1;
        }
        if (res.isZero()) {
            return 0;
        }
        return TimeUnit.MILLISECONDS.convert(res.getDurationAmount(), res.getTimeUnit());
    }

    private long getUpdatedExpireTime() {
        if (policy == null) {
            return -1;
        }
        Duration res = policy.getExpiryForUpdate();
        if (res == null || res.isEternal()) {
            return -1;
        }
        if (res.isZero()) {
            return 0;
        }
        return TimeUnit.MILLISECONDS.convert(res.getDurationAmount(), res.getTimeUnit());
    }

    private long getAccessExpireTime() {
        if (policy == null) {
            return -1;
        }
        Duration res = policy.getExpiryForAccess();
        if (res == null || res.isEternal()) {
            return -1;
        }
        if (res.isZero()) {
            return 0;
        }
        return TimeUnit.MILLISECONDS.convert(res.getDurationAmount(), res.getTimeUnit());
    }

    /**
     * Sets statistics
     */
    final void setStatisticsEnabled(boolean enabled) {
        if (enabled) {
            JMXUtils.registerStatisticsMXBean(this, statisticsMXBean);
        } else {
            JMXUtils.unregisterStatisticsMXBean(this);
        }
        configuration.setStatisticsEnabled(enabled);
    }

    /**
     * Sets management enablement
     *
     * @param enabled true if management should be enabled
     */
    final void setManagementEnabled(boolean enabled) {
        if (enabled) {
            JMXUtils.registerConfigurationMXBean(this, configurationMXBean);
        } else {
            JMXUtils.unregisterConfigurationMXBean(this);
        }
        configuration.setManagementEnabled(enabled);
    }

    private void closeCustomization(Object object) {
        if (object != null && object instanceof AutoCloseable) {
            try {
                ((AutoCloseable) object).close();
            } catch (Exception suppress) {
                LOG.log(Level.SEVERE, "error while closing customization " + object, suppress);
            }
        }
    }
    private static final Logger LOG = Logger.getLogger(BlazingCacheCache.class.getName());

    private static class EntryProcessorResultImpl<T> implements EntryProcessorResult<T> {

        private final T value;
        private final EntryProcessorException exception;

        public EntryProcessorResultImpl(T value, EntryProcessorException exception) {
            this.value = value;
            this.exception = exception;
        }

        @Override
        public T get() throws EntryProcessorException {
            if (exception != null) {
                throw exception;
            }
            return value;
        }

    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        checkClosed();
        if (keys == null || entryProcessor == null) {
            throw new NullPointerException();
        }
        HashMap<K, EntryProcessorResult<T>> map = new HashMap<>();
        for (K key : keys) {
            EntryProcessorResult<T> result;
            try {
                T t = invoke(key, entryProcessor, arguments);
                result = t == null ? null : new EntryProcessorResultImpl<>(t, null);
            } catch (Exception e) {
                result = new EntryProcessorResultImpl<>(null, new EntryProcessorException(e));
            }
            if (result != null) {
                map.put(key, result);
            }
        }

        return map;
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
        if (closed) {
            return;
        }
        clear();
        closeCustomizations();
        setStatisticsEnabled(false);
        setManagementEnabled(false);
        closed = true;
    }

    private void closeCustomizations() {
        closeCustomization(cacheLoader);
        closeCustomization(cacheWriter);
        closeCustomization(policy);
        closeCustomization(keysSerializer);
        closeCustomization(valuesSerializer);
        listeners.forEach((l) -> {
            closeCustomization(l);
        });

    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        checkClosed();
        for (BlazingCacheCacheEntryListenerWrapper listenerWrapper : listeners) {
            if (listenerWrapper.configuration.equals(cacheEntryListenerConfiguration)) {
                throw new IllegalArgumentException("configuration " + cacheEntryListenerConfiguration + " already used");
            }
        }
        configureListener(cacheEntryListenerConfiguration);
        this.configuration.addCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);

    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        checkClosed();
        List<BlazingCacheCacheEntryListenerWrapper<K, V>> newList = new ArrayList<>();
        boolean _needPreviuosValueForListeners = false;
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listenerWrapper : listeners) {
            if (!listenerWrapper.configuration.equals(cacheEntryListenerConfiguration)) {
                newList.add(listenerWrapper);
                _needPreviuosValueForListeners = _needPreviuosValueForListeners | listenerWrapper.needPreviousValue;
            }
        }
        listeners = newList;
        needPreviuosValueForListeners = _needPreviuosValueForListeners || policy != null;
        this.configuration.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

    @SuppressWarnings("unchecked")
    private void configureListener(CacheEntryListenerConfiguration<K, V> listenerConfig) {
        BlazingCacheCacheEntryListenerWrapper wrapper = new BlazingCacheCacheEntryListenerWrapper(
                listenerConfig.isSynchronous(),
                listenerConfig.isOldValueRequired(),
                listenerConfig.getCacheEntryListenerFactory().create(),
                listenerConfig.getCacheEntryEventFilterFactory() != null ? listenerConfig.getCacheEntryEventFilterFactory().create() : null,
                listenerConfig,
                this
        );
        listeners.add(wrapper);
        needPreviuosValueForListeners = needPreviuosValueForListeners | wrapper.needPreviousValue || policy != null;
    }

    private static class EntryIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

        private K currentKey;
        private final Iterator<K> keysIterator;
        private final BlazingCacheCache<K, V> parent;

        public EntryIterator(Iterator<K> keysIterator, BlazingCacheCache<K, V> parent) {
            this.keysIterator = keysIterator;
            this.parent = parent;
        }

        @Override
        public boolean hasNext() {
            return keysIterator.hasNext();
        }

        @Override
        public Entry<K, V> next() {
            currentKey = keysIterator.next();
            parent.cacheHits.incrementAndGet();
            String serializedKey = parent.cacheName + "#" + parent.keysSerializer.serialize(currentKey);
            BlazingCacheEntry<K, V> res = new BlazingCacheEntry<>(currentKey, parent.getNoFetch(serializedKey));
            boolean validAfterAccess = parent.handleEntryAccessed(serializedKey, null);
            if (!validAfterAccess) {
                try {
                    parent.client.invalidate(serializedKey);
                } catch (InterruptedException err) {
                    Thread.currentThread().interrupt();
                    throw new javax.cache.CacheException(err);
                }
            }
            return res;
        }

        @Override
        public void remove() {
            if (currentKey == null) {
                throw new IllegalStateException();
            }
            parent.remove(currentKey);
            currentKey = null;
        }
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        checkClosed();
        int prefixLen = (cacheName + "#").length();
        Set<K> localKeys = client
                .getLocalKeySetByPrefix(cacheName + "#")
                .stream()
                .map(s -> {
                    String noPrefix = s.substring(prefixLen);
                    K key = keysSerializer.deserialize(noPrefix);
                    return (K) key;
                }).collect(Collectors.toSet());
        Iterator<K> keysIterator = localKeys.iterator();
        return new EntryIterator(keysIterator, this);

    }

    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong cachePuts = new AtomicLong();
    private final AtomicLong cacheRemovals = new AtomicLong();

    void clearStatistics() {
        cacheHits.set(0);
        cacheMisses.set(0);
        cachePuts.set(0);
        cacheRemovals.set(0);
    }

    long getCacheHits() {
        return cacheHits.get();
    }

    long getCacheMisses() {
        return cacheMisses.get();
    }

    long getCacheGets() {
        return cacheHits.get() + cacheMisses.get();
    }

    long getCachePuts() {
        return cachePuts.get();
    }

    long getCacheRemovals() {
        return cacheRemovals.get();
    }

    long getCacheEvictions() {
        return 0;
    }

    float getAverageGetTime() {
        return 0;
    }

    float getAveragePutTime() {
        return 0;
    }

    float getAverageRemoveTime() {
        return 0;
    }

    public BlazingCacheConfigurationMXBean<K, V> getConfigurationMXBean() {
        return configurationMXBean;
    }

    public BlazingCacheStatisticsMXBean<K, V> getStatisticsMXBean() {
        return statisticsMXBean;
    }

}
