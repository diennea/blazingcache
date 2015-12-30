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
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
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
    private final MutableConfiguration<K, V> configuration;
    private final CacheManager cacheManager;
    private volatile boolean closed;
    private final long defaultTtl;
    private final CacheLoader<K, V> cacheLoader;
    private final CacheWriter<K, V> cacheWriter;
    private final Class<V> valueType;
    private final Class<K> keyType;
    private final boolean isReadThrough;
    private final boolean isWriteThrough;
    private boolean needPreviuosValueForListeners = false;
    private List<BlazingCacheCacheEntryListenerWrapper> listeners = new ArrayList<>();

    public BlazingCacheCache(String cacheName, CacheClient client, CacheManager cacheManager, Serializer<Object, String> keysSerializer, Serializer<Object, byte[]> valuesSerializer, boolean usefetch, Configuration<K, V> configuration) {
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
            if (policy == null || policy.getExpiryForAccess() == null || policy.getExpiryForAccess().isEternal()) {
                defaultTtl = -1;
            } else if (policy.getExpiryForAccess().isZero()) {
                defaultTtl = 1;
            } else {
                defaultTtl = policy.getExpiryForAccess().getTimeUnit().convert(policy.getExpiryForAccess().getDurationAmount(), TimeUnit.MILLISECONDS);
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
            defaultTtl = -1;
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
            listener.onEntryCreated(key, value);
        }
    }

    private void fireEntryUpdated(K key, V prevValue, V value) {
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listener : listeners) {
            listener.onEntryUpdated(key, prevValue, value);
        }
    }

    private void fireEntryRemoved(K key, V prevValue) {
        for (BlazingCacheCacheEntryListenerWrapper<K, V> listener : listeners) {
            listener.onEntryRemoved(key, prevValue);
        }
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
                return (V) valuesSerializer.deserialize(result.getSerializedData());
            } else {
                if (allowLoader && cacheLoader != null && isReadThrough) {
                    V loaded = cacheLoader.load(key);
                    if (loaded != null) {
                        client.put(keysSerializer.serialize(key), valuesSerializer.serialize(loaded), defaultTtl);
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
        if (cacheLoader == null || keys == null || keys.isEmpty()) {
            completionListener.onCompletion();
            return;
        }

        try {
            Set<K> toLoad;
            if (replaceExistingValues) {
                toLoad = (Set<K>) keys;
            } else {
                toLoad = new HashSet<>();
                for (K k : keys) {
                    if (k != null && !containsKey(k)) {
                        toLoad.add(k);
                    }
                }
            }
            if (!toLoad.isEmpty()) {
                Map<K, V> loaded = cacheLoader.loadAll(toLoad);
                if (loaded != null) {
                    for (Map.Entry<K, V> loaded_entry : loaded.entrySet()) {
                        K key = loaded_entry.getKey();
                        V value = loaded_entry.getValue();
                        if (value != null) {
                            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                            if (needPreviuosValueForListeners) {
                                V actual = getNoFetch(key);
                                client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
                                if (actual != null) {
                                    fireEntryCreated(key, value);
                                } else {
                                    fireEntryUpdated(key, actual, value);
                                }
                            } else {
                                client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
                            }
                        }
                    }
                }
            }
            completionListener.onCompletion();
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            completionListener.onException(err);
        } catch (CacheException ex) {
            completionListener.onException(ex);
        }
    }

    private void runtimeCheckType(K key, V value) {
        if (keyType != null && !keyType.isInstance(key)) {
            throw new ClassCastException(key.getClass().toString());
        }
        if (valueType != null && !valueType.isInstance(value)) {
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
            if (isWriteThrough) {
                cacheWriter.write(new BlazingCacheEntry<>(key, value));
            }
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
            if (isWriteThrough) {
                cacheWriter.write(new BlazingCacheEntry<>(key, value));
            }
            if (actual == null) {
                fireEntryCreated(key, value);
            } else {
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
            Collection<Cache.Entry<? extends K, ? extends V>> entries = new ArrayList<>();
            for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                runtimeCheckType(key, value);
                if (needPreviuosValueForListeners) {
                    V previousForListener = getNoFetch(key);
                    String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                    client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
                    entries.add(new BlazingCacheEntry<>(key, value));
                    if (previousForListener == null) {
                        fireEntryCreated(key, value);
                    } else {
                        fireEntryUpdated(key, previousForListener, value);
                    }
                } else {
                    String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
                    client.put(serializedKey, valuesSerializer.serialize(value), defaultTtl);
                    entries.add(new BlazingCacheEntry<>(key, value));
                }

            }
            if (isWriteThrough) {
                cacheWriter.writeAll(entries);
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
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            client.invalidate(serializedKey);
            if (allowWriteThrough && isWriteThrough) {
                cacheWriter.delete(key);
            }
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
        if (key == null) {
            throw new NullPointerException();
        }
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            if (Objects.equals(actual, oldValue)) {
                client.invalidate(serializedKey);
                if (isWriteThrough) {
                    cacheWriter.delete(key);
                }
                fireEntryRemoved(key, actual);
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
        checkClosed();
        if (key == null) {
            throw new NullPointerException();
        }
        try {
            String serializedKey = cacheName + "#" + keysSerializer.serialize(key);
            V actual = getNoFetch(key);
            client.invalidate(serializedKey);
            if (isWriteThrough) {
                cacheWriter.delete(key);
            }
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
        if (containsKey(key) && Objects.equals((Object) get(key, false), oldValue)) {
            put(key, newValue);
            return true;
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
        for (K key : keys) {
            remove(key, false);
        }
        if (isWriteThrough) {
            cacheWriter.deleteAll(keys);
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
            client.invalidateByPrefix(cacheName + "#");
            if (localKeys != null && isWriteThrough) {
                cacheWriter.deleteAll(localKeys);
            }
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
        throw new UnsupportedOperationException("Not supported yet. (invoke)"); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        checkClosed();
        throw new UnsupportedOperationException("Not supported yet. (invokeAll)"); //To change body of generated methods, choose Tools | Templates.
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
        }
        return null;
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
                    return (K) keysSerializer.deserialize(s.substring(prefixLen));
                }).collect(Collectors.toSet());
        Iterator<K> keysIterator = localKeys.iterator();
        return new EntryIterator(keysIterator, this);

    }

}
