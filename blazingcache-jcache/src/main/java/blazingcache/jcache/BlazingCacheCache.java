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
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
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

    private final String cacheName;
    private final CacheClient client;
    private final Serializer<K, String> keysSerializer;
    private final Serializer<V, byte[]> valuesSerializer;
    private final boolean usefetch;
    private final MutableConfiguration<K, V> configuration;
    private final CacheManager cacheManager;
    private volatile boolean closed;
    private final long createdExpireTime;
    private final long updatedExpireTime;
    private final long accessExpireTime;
    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;
    private final Class<V> valueType;
    private final Class<K> keyType;
    private final boolean isReadThrough;
    private final boolean isWriteThrough;
    private boolean needPreviuosValueForListeners = false;
    private List<BlazingCacheCacheEntryListenerWrapper> listeners = new ArrayList<>();

    public BlazingCacheCache(String cacheName, CacheClient client, CacheManager cacheManager, Serializer<K, String> keysSerializer, Serializer<V, byte[]> valuesSerializer, boolean usefetch, Configuration<K, V> configuration) {
        this.cacheName = cacheName;
        this.cacheManager = cacheManager;
        this.client = client;
        this.keysSerializer = keysSerializer;
        this.valuesSerializer = valuesSerializer;
        this.valueType = configuration.getValueType();
        this.keyType = configuration.getKeyType();
        this.usefetch = usefetch;
        if (configuration instanceof CompleteConfiguration) {
            this.configuration = new MutableConfiguration<>((CompleteConfiguration<K, V>) configuration);
            CompleteConfiguration<K, V> cc = (CompleteConfiguration<K, V>) configuration;
            ExpiryPolicy policy = (ExpiryPolicy) cc.getExpiryPolicyFactory().create();
            if (policy == null) {
                createdExpireTime = -1;
                updatedExpireTime = -1;
                accessExpireTime = -1;
            } else {
                if (policy.getExpiryForCreation() == null || policy.getExpiryForCreation().isEternal()) {
                    createdExpireTime = -1;
                } else if (policy.getExpiryForCreation().isZero()) {
                    createdExpireTime = 0;
                } else {
                    createdExpireTime = policy.getExpiryForCreation().getTimeUnit().convert(policy.getExpiryForCreation().getDurationAmount(), TimeUnit.MILLISECONDS);
                }
                if (policy.getExpiryForUpdate() == null || policy.getExpiryForUpdate().isEternal()) {
                    updatedExpireTime = -1;
                } else if (policy.getExpiryForUpdate().isZero()) {
                    updatedExpireTime = 0;
                } else {
                    updatedExpireTime = policy.getExpiryForUpdate().getTimeUnit().convert(policy.getExpiryForUpdate().getDurationAmount(), TimeUnit.MILLISECONDS);
                }
                if (policy.getExpiryForAccess() == null || policy.getExpiryForAccess().isEternal()) {
                    accessExpireTime = -1;
                } else if (policy.getExpiryForAccess().isZero()) {
                    accessExpireTime = 0;
                } else {
                    accessExpireTime = policy.getExpiryForAccess().getTimeUnit().convert(policy.getExpiryForAccess().getDurationAmount(), TimeUnit.MILLISECONDS);
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
            if (cc.getCacheEntryListenerConfigurations() != null) {
                for (CacheEntryListenerConfiguration<K, V> listenerConfig : cc.getCacheEntryListenerConfigurations()) {
                    configureListener(listenerConfig);
                }
            }
        } else {
            this.configuration = new MutableConfiguration<K, V>()
                    .setTypes(configuration.getKeyType(), configuration.getValueType())
                    .setStoreByValue(configuration.isStoreByValue());
            createdExpireTime = -1;
            updatedExpireTime = -1;
            accessExpireTime = -1;
            cacheLoader = null;
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

    private V getNoFetch(K key) {
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
        checkClosed();
        if (key == null) {
            throw new NullPointerException();
        }
        return get(key, true);
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

    private void handleEntryAccessed(String serializedKey) {
        if (accessExpireTime >= 0) {
            client.touchEntry(serializedKey, nowPlusDuration(accessExpireTime));
        }
    }

    private static long nowPlusDuration(long duration) {
        if (duration < 0) {
            return -1;
        }
        return System.currentTimeMillis() + duration;
    }

    private V get(K key, boolean allowLoader) {
        checkClosed();
        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
        try {
            CacheEntry result;
            if (usefetch) {
                result = client.fetch(serializedKey);
            } else {
                result = client.get(serializedKey);
            }
            if (result != null) {
                handleEntryAccessed(serializedKey);
                return (V) valuesSerializer.deserialize(result.getSerializedData());
            } else {
                if (allowLoader && cacheLoader != null && isReadThrough) {
                    V loaded;
                    try {
                        loaded = cacheLoader.load(key);
                    } catch (Exception err) {
                        throw new CacheLoaderException(err);
                    }
                    if (loaded != null) {
                        if (createdExpireTime != 0) {
                            client.put(serializedKey, valuesSerializer.serialize(loaded), nowPlusDuration(createdExpireTime));
                        }
                        fireEntryCreated(key, loaded);
                        return loaded;
                    }
                }
                return null;
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }

    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
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

                String serializedKey = cacheName + "#" + keysSerializer.serialize(key);

                V r = null;
                CacheEntry result;
                if (usefetch) {
                    result = client.fetch(serializedKey);
                } else {
                    result = client.get(serializedKey);
                }
                if (result != null) {
                    r = (V) valuesSerializer.deserialize(result.getSerializedData());
                } else if (cacheLoader != null && isReadThrough) {
                    keysToLoad.add(key);
                }

                if (r != null) {
                    handleEntryAccessed(serializedKey);
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
                        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                        if (createdExpireTime != 0) {
                            client.put(serializedKey, valuesSerializer.serialize(loaded), nowPlusDuration(createdExpireTime));
                        }

                        fireEntryCreated(key, loaded);
                        map_result.put(entry.getKey(), entry.getValue());
                    }
                }
            }
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
        String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
        return client.get(serializedKey) != null;
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
                            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                            if (needPreviuosValueForListeners) {
                                V actual = getNoFetch(key);
                                if (actual != null) {
                                    if (createdExpireTime != 0) {
                                        client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(createdExpireTime));
                                        fireEntryCreated(key, value);
                                    }
                                } else {
                                    client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(updatedExpireTime));
                                    fireEntryUpdated(key, actual, value);
                                }
                            } else if (createdExpireTime != 0) {
                                client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(createdExpireTime));
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
        checkClosed();

        if (key == null || value == null) {
            throw new NullPointerException();
        }

        runtimeCheckType(key, value);

        if (needPreviuosValueForListeners) {
            getAndPut(key, value);
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(updatedExpireTime));
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            throw new javax.cache.CacheException(err);
        } catch (CacheException err) {
            throw new javax.cache.CacheException(err);
        }
    }

    @Override
    public V getAndPut(K key, V value) {
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);

            if (actual == null) {
                if (createdExpireTime != 0) {
                    client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(createdExpireTime));
                    fireEntryCreated(key, value);
                }
            } else {
                client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(updatedExpireTime));
                fireEntryUpdated(key, actual, value);
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
        try {
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (key == null || value == null) {
                    throw new NullPointerException();
                }
            }
            if (isWriteThrough) {
                Collection<Cache.Entry<? extends K, ? extends V>> entries = new ArrayList<>();
                for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                    K key = entry.getKey();
                    V value = entry.getValue();
                    entries.add(new BlazingCacheEntry<>(key, value));
                }

                try {
                    cacheWriter.writeAll(entries);
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                runtimeCheckType(key, value);
                if (needPreviuosValueForListeners) {
                    V previousForListener = getNoFetch(key);
                    String serializedKey = cacheName + "#" + keysSerializer.serialize(key);

                    if (previousForListener == null) {
                        if (createdExpireTime != 0) {
                            client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(createdExpireTime));
                            fireEntryCreated(key, value);
                        }
                    } else {
                        client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(updatedExpireTime));
                        fireEntryUpdated(key, previousForListener, value);
                    }
                } else if (createdExpireTime != 0) {
                    String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                    client.put(serializedKey, valuesSerializer.serialize(value), nowPlusDuration(createdExpireTime));
                }

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
            put(key, value);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean remove(K key) {
        checkClosed();
        return remove(key, true);
    }

    private boolean remove(K key, boolean allowWriteThrough) {
        if (key == null) {
            throw new NullPointerException();
        }
        try {
            V actual = getNoFetch(key);
            if (actual == null) {
                return false;
            }
            if (allowWriteThrough && isWriteThrough) {
                try {
                    cacheWriter.delete(key);
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.invalidate(serializedKey);
            fireEntryRemoved(key, actual);
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            if (Objects.equals(actual, oldValue)) {
                if (isWriteThrough) {
                    try {
                        cacheWriter.delete(key);
                    } catch (Exception err) {
                        throw new CacheWriterException(err);
                    }
                }
                client.invalidate(serializedKey);

                fireEntryRemoved(key, actual);
                return true;
            }
            handleEntryAccessed(serializedKey);
            return false;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            client.invalidate(serializedKey);
            if (actual != null) {
                fireEntryRemoved(key, actual);
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

        if (containsKey(key)) {
            if (Objects.equals((Object) get(key, false), oldValue)) {
                put(key, newValue);
                return true;
            } else {
                String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                handleEntryAccessed(serializedKey);
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean replace(K key, V value) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        if (containsKey(key)) {
            put(key, value);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public V getAndReplace(K key, V value) {
        checkClosed();
        if (key == null || value == null) {
            throw new NullPointerException();
        }
        if (containsKey(key)) {
            V oldValue = get(key, false);
            put(key, value);
            return oldValue;
        } else {
            return null;
        }
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        checkClosed();
        if (isWriteThrough) {
            try {
                cacheWriter.deleteAll(keys);
            } catch (Exception err) {
                throw new CacheWriterException(err);
            }
        }
        for (K key : keys) {
            remove(key, false);
        }

    }

    @Override
    public void removeAll() {
        checkClosed();
        try {
            Set<K> localKeys;
            Map<K, V> previousValuesForListener = null;
            if (isWriteThrough || needPreviuosValueForListeners) {
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
                        V value = getNoFetch(key);
                        previousValuesForListener.put(key, value);
                    }
                }
            } else {
                localKeys = null;
            }
            if (localKeys != null && isWriteThrough) {
                try {
                    cacheWriter.deleteAll(localKeys);
                } catch (Exception err) {
                    throw new CacheWriterException(err);
                }
            }
            client.invalidateByPrefix(cacheName + "#");

            if (previousValuesForListener != null) {
                for (Map.Entry<K, V> entry : previousValuesForListener.entrySet()) {
                    fireEntryRemoved(entry.getKey(), entry.getValue());
                }
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
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        return (C) configuration;
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        checkClosed();
        if (key == null || entryProcessor == null) {
            throw new NullPointerException();
        }
        try {
            V actualValue = get(key);
            BlazingCacheCacheMutableEntry<K, V> entry = new BlazingCacheCacheMutableEntry<>(key, actualValue);
            T returnValue = entryProcessor.process(entry, arguments);
            if (entry.isRemoved()) {
                remove(key);
            } else if (entry.isUpdated()) {
                put(key, entry.getValue());
            }
            return returnValue;
        } catch (javax.cache.CacheException err) {
            throw err;
        } catch (Exception err) {
            throw new EntryProcessorException(err);
        }
    }

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
        for (K key : keys) {
            if (key == null) {
                throw new NullPointerException();
            }
        }
        try {
            Map<K, EntryProcessorResult<T>> result = new HashMap<>();
            Map<K, V> actualValues = getAll(keys);
            Set<K> keysToRemove = new HashSet<>();
            Map<K, V> valuesToPut = new HashMap<>();
            for (K key : keys) {
                V actualValue = actualValues.get(key);
                BlazingCacheCacheMutableEntry<K, V> entry = new BlazingCacheCacheMutableEntry<>(key, actualValue);
                T returnValue = null;
                EntryProcessorException exception = null;
                try {
                    returnValue = entryProcessor.process(entry, arguments);
                } catch (Exception err) {
                    exception = new EntryProcessorException(err);
                }
                if (exception == null) {
                    if (entry.isRemoved()) {
                        keysToRemove.add(key);
                    } else if (entry.isUpdated()) {
                        valuesToPut.put(key, entry.getValue());
                    }
                    if (returnValue != null) {
                        result.put(key, new EntryProcessorResultImpl(returnValue, null));
                    }
                } else {
                    result.put(key, new EntryProcessorResultImpl<>(null, exception));
                }

            }

            // cache mutations
            if (!keysToRemove.isEmpty()) {
                removeAll(keysToRemove);
            }
            if (!valuesToPut.isEmpty()) {
                putAll(valuesToPut);
            }
            return result;
        } catch (javax.cache.CacheException err) {
            throw err;
        }
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
        List<BlazingCacheCacheEntryListenerWrapper> newList = new ArrayList<>();
        boolean _needPreviuosValueForListeners = false;
        for (BlazingCacheCacheEntryListenerWrapper listenerWrapper : listeners) {
            if (!listenerWrapper.configuration.equals(cacheEntryListenerConfiguration)) {
                newList.add(listenerWrapper);
                _needPreviuosValueForListeners = _needPreviuosValueForListeners | listenerWrapper.needPreviousValue;
            }
        }
        listeners = newList;
        needPreviuosValueForListeners = _needPreviuosValueForListeners;
        if (createdExpireTime != updatedExpireTime) {
            needPreviuosValueForListeners = true;
        }
        this.configuration.removeCacheEntryListenerConfiguration(cacheEntryListenerConfiguration);
    }

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
        needPreviuosValueForListeners = needPreviuosValueForListeners | wrapper.needPreviousValue;
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
            return new BlazingCacheEntry<>(currentKey, parent.getNoFetch(currentKey));
        }

        @Override
        public void remove() {
            if (currentKey == null) {
                throw new IllegalStateException();
            }
            parent.remove(currentKey);
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

}
