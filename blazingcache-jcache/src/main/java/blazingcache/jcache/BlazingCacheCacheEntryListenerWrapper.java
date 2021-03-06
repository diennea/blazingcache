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

import java.util.Arrays;
import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;

/**
 * Wrapper for CacheEntryListenerWrapper;
 *
 * @author enrico.olivelli
 */
final class BlazingCacheCacheEntryListenerWrapper<K, V> implements AutoCloseable {

    final boolean synchronous;
    final boolean oldValueRequired;
    final boolean needPreviousValue;
    final CacheEntryListener<K, V> listener;
    final CacheEntryEventFilter<K, V> filter;
    final CacheEntryListenerConfiguration<K, V> configuration;
    final boolean onCreate;
    final boolean onUpdate;
    final boolean onRemove;
    final BlazingCacheCache<K, V> parent;

    BlazingCacheCacheEntryListenerWrapper(boolean synchronous, boolean oldValueRequired, CacheEntryListener<K, V> listener, CacheEntryEventFilter<K, V> filter, CacheEntryListenerConfiguration<K, V> configuration, BlazingCacheCache<K, V> parent) {
        this.synchronous = synchronous;
        this.parent = parent;
        this.oldValueRequired = oldValueRequired;
        this.listener = listener;
        this.filter = filter;
        this.configuration = configuration;
        this.onCreate = listener instanceof CacheEntryCreatedListener;
        this.onUpdate = listener instanceof CacheEntryUpdatedListener;
        this.onRemove = listener instanceof CacheEntryRemovedListener;
        this.needPreviousValue = oldValueRequired || onRemove || onUpdate;
    }

    private class BlazingCacheCacheEntryEvent extends CacheEntryEvent<K, V> {

        private final K key;
        private final V oldValue;
        private final V value;
        private final boolean oldValuePresent;

        public BlazingCacheCacheEntryEvent(K key, V oldValue, V value, Cache source, EventType eventType, boolean oldValuePresent) {
            super(source, eventType);
            this.key = key;
            this.oldValue = oldValue;
            this.value = value;
            this.oldValuePresent = oldValuePresent && needPreviousValue;
        }

        @Override
        public V getOldValue() {
            return oldValue;
        }

        @Override
        public boolean isOldValueAvailable() {
            return oldValuePresent;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public <T> T unwrap(Class<T> clazz) {
            if (clazz.isInstance(this)) {
                return (T) this;
            } else {
                throw new IllegalArgumentException();
            }
        }

    }

    void onEntryCreated(K key, V value) {
        if (onCreate) {
            BlazingCacheCacheEntryEvent event = new BlazingCacheCacheEntryEvent(key, null, value, parent, EventType.CREATED, false);
            if (filter != null && !filter.evaluate(event)) {
                return;
            }
            ((CacheEntryCreatedListener) listener).onCreated(Arrays.asList(event));
        }
    }

    void onEntryUpdated(K key, V oldValue, V value) {
        if (onUpdate) {
            BlazingCacheCacheEntryEvent event = new BlazingCacheCacheEntryEvent(key, oldValue, value, parent, EventType.UPDATED, true);
            if (filter != null && !filter.evaluate(event)) {
                return;
            }
            ((CacheEntryUpdatedListener) listener).onUpdated(Arrays.asList(event));
        }
    }

    void onEntryRemoved(K key, V oldValue) {
        if (onRemove) {
            BlazingCacheCacheEntryEvent event = new BlazingCacheCacheEntryEvent(key, oldValue, oldValue, parent, EventType.REMOVED, true);
            if (filter != null && !filter.evaluate(event)) {
                return;
            }
            ((CacheEntryRemovedListener) listener).onRemoved(Arrays.asList(event));
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (listener != null && listener instanceof AutoCloseable) {
                ((AutoCloseable) listener).close();
            }
        } finally {
            if (filter != null && filter instanceof AutoCloseable) {
                ((AutoCloseable) filter).close();
            }
        }
    }

}
