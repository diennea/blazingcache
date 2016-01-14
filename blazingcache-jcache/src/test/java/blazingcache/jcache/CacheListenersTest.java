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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.REMOVED;
import static javax.cache.event.EventType.UPDATED;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import org.junit.After;
import static org.junit.Assert.assertEquals;

/**
 * Test for listeners
 *
 * @author enrico.olivelli
 */
public class CacheListenersTest {

    @After
    public void clear() {
        Caching.getCachingProvider().close();
    }

    @Before
    public void setupLogger() throws Exception {
        Level level = Level.SEVERE;
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
                e.printStackTrace();
            }
        });
        java.util.logging.LogManager.getLogManager().reset();
        ConsoleHandler ch = new ConsoleHandler();
        ch.setLevel(level);
        SimpleFormatter f = new SimpleFormatter();
        ch.setFormatter(f);
        java.util.logging.Logger.getLogger("").setLevel(level);
        java.util.logging.Logger.getLogger("").addHandler(ch);
    }

    @Test
    public void testCreateListenerSynch() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Map<String, String> created = new HashMap<>();
            CacheEntryCreatedListener<String, String> listener = new CacheEntryCreatedListener<String, String>() {
                @Override
                public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends String> e : events) {
                        created.put(e.getKey(), e.getValue());
                    }
                }
            };

            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                            new FactoryBuilder.SingletonFactory(listener), null, true, true)
                    );

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);

            String key = "key";
            cache.put(key, "value");
            assertEquals("value", created.get(key));
        }
    }

    @Test
    public void testUpdateListenerSynch() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Map<String, String> updated = new HashMap<>();
            Map<String, String> created = new HashMap<>();
            Map<String, String> removed = new HashMap<>();
            CacheEntryUpdatedListener<String, String> listener = new CacheEntryUpdatedListener<String, String>() {
                @Override
                public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends String>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends String> e : events) {
                        if (updated.containsKey(e.getKey())) {
                            throw new RuntimeException();
                        }
                        updated.put(e.getKey(), e.getValue());
                    }
                }
            };

            CacheEntryCreatedListener<String, String> listener2 = new CacheEntryCreatedListener<String, String>() {
                @Override
                public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends String> e : events) {
                        created.put(e.getKey(), e.getValue());
                    }
                }
            };

            CacheEntryRemovedListener<String, String> listener3 = new CacheEntryRemovedListener<String, String>() {
                @Override
                public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends String>> events) throws CacheEntryListenerException {
                    for (CacheEntryEvent<? extends String, ? extends String> e : events) {
                        removed.put(e.getKey(), e.getOldValue());
                    }
                }
            };

            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                            new FactoryBuilder.SingletonFactory(listener), null, true, true)
                    )
                    .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                            new FactoryBuilder.SingletonFactory(listener2), null, true, true)
                    )
                    .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                            new FactoryBuilder.SingletonFactory(listener3), null, true, true)
                    );

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);

            String key = "key";
            cache.put(key, "value");
            cache.put(key, "value2");
            assertEquals("value2", updated.get(key));
            assertEquals("value", created.get(key));
            cache.remove(key);
            assertEquals("value2", removed.get(key));
            cache.put(key, "value3");
            assertEquals("value3", created.get(key));
        }
    }

    private static class MyCacheEntryEventFilter implements CacheEntryEventFilter<Long, String>, Serializable {

        @Override
        public boolean evaluate(
                CacheEntryEvent<? extends Long, ? extends String> event)
                throws CacheEntryListenerException {
            return event.getValue().contains("a")
                    || event.getValue().contains("e")
                    || event.getValue().contains("i")
                    || event.getValue().contains("o")
                    || event.getValue().contains("u");
        }
    }

    /**
     * Test listener
     *
     * @param <K>
     * @param <V>
     */
    public static class MyCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>,
            CacheEntryUpdatedListener<K, V>, CacheEntryExpiredListener<K, V>,
            CacheEntryRemovedListener<K, V>, Serializable,
            AutoCloseable {

        AtomicInteger created = new AtomicInteger();
        AtomicInteger updated = new AtomicInteger();
        AtomicInteger removed = new AtomicInteger();

        ArrayList<CacheEntryEvent<K, V>> entries = new ArrayList<CacheEntryEvent<K, V>>();

        public int getCreated() {
            return created.get();
        }

        public int getUpdated() {
            return updated.get();
        }

        public int getRemoved() {
            return removed.get();
        }

        public ArrayList<CacheEntryEvent<K, V>> getEntries() {
            return entries;
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : events) {
                assertEquals(CREATED.toString(), event.getEventType().toString());
                created.incrementAndGet();

                // added for code coverage.
                event.getKey();
                event.getValue();
                event.getSource();
            }
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {
            //SKIP: we don't count expiry events as they can occur asynchronously
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {            
            for (CacheEntryEvent<? extends K, ? extends V> event : events) {
                
                assertEquals(REMOVED.toString(), event.getEventType().toString());
                removed.incrementAndGet();
                event.getKey();
                if (event.isOldValueAvailable()) {
                    event.getOldValue();
                }
            }
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends K, ? extends V> event : events) {
                assertEquals(UPDATED.toString(), event.getEventType().toString());
                updated.incrementAndGet();
                event.getKey();
                if (event.isOldValueAvailable()) {
                    event.getOldValue();
                }
            }
        }

        @Override
        public void close() throws Exception {
            // added for code coverage
        }
    }

    @Test
    public void testFilteredListener() throws InterruptedException {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Map<String, String> created = new HashMap<>();

            MutableConfiguration<Long, String> config
                    = new MutableConfiguration<Long, String>()
                    .setTypes(Long.class, String.class);

            Cache<Long, String> cache = cacheManager.createCache("simpleCache", config);

            MyCacheEntryListener<Long, String> filteredListener = new MyCacheEntryListener<>();
            CacheEntryListenerConfiguration<Long, String> listenerConfiguration
                    = new MutableCacheEntryListenerConfiguration<Long, String>(
                            FactoryBuilder.factoryOf(filteredListener),
                            FactoryBuilder.factoryOf(new MyCacheEntryEventFilter()),
                            true, true);
            cache.registerCacheEntryListener(listenerConfiguration);

            assertEquals(0, filteredListener.getCreated());
            assertEquals(0, filteredListener.getUpdated());
            assertEquals(0, filteredListener.getRemoved());

            cache.put(1l, "Sooty");
            assertEquals(1, filteredListener.getCreated());
            assertEquals(0, filteredListener.getUpdated());
            assertEquals(0, filteredListener.getRemoved());

            Map<Long, String> entries = new HashMap<Long, String>();
            entries.put(2l, "Lucky");
            entries.put(3l, "Bryn");
            cache.putAll(entries);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(0, filteredListener.getUpdated());
            assertEquals(0, filteredListener.getRemoved());

            cache.put(1l, "Zyn");
            assertEquals(2, filteredListener.getCreated());
            assertEquals(0, filteredListener.getUpdated());
            assertEquals(0, filteredListener.getRemoved());

            cache.remove(2l);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(0, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.replace(1l, "Fred");
            assertEquals(2, filteredListener.getCreated());
            assertEquals(1, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.replace(3l, "Bryn", "Sooty");
            assertEquals(2, filteredListener.getCreated());
            assertEquals(2, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.get(1L);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(2, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            //containsKey is not a read for filteredListener purposes.
            cache.containsKey(1L);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(2, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            //iterating should cause read events on non-expired entries
            for (Cache.Entry<Long, String> entry : cache) {
                String value = entry.getValue();
                System.out.println(value);
            }
            assertEquals(2, filteredListener.getCreated());
            assertEquals(2, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.getAndPut(1l, "Pistachio");
            assertEquals(2, filteredListener.getCreated());
            assertEquals(3, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            Set<Long> keys = new HashSet<Long>();
            keys.add(1L);
            cache.getAll(keys);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(3, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.getAndReplace(1l, "Prince");
            assertEquals(2, filteredListener.getCreated());
            assertEquals(4, filteredListener.getUpdated());
            assertEquals(1, filteredListener.getRemoved());

            cache.getAndRemove(1l);
            assertEquals(2, filteredListener.getCreated());
            assertEquals(4, filteredListener.getUpdated());
            assertEquals(2, filteredListener.getRemoved());

            assertEquals(2, filteredListener.getCreated());
            assertEquals(4, filteredListener.getUpdated());
            assertEquals(2, filteredListener.getRemoved());
        }
    }

}
