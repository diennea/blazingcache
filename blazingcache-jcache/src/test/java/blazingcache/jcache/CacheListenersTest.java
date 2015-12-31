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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import org.junit.After;

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

}
