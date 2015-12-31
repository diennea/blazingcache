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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import org.junit.After;

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class CacheWriterTest {

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

    public static class MockCacheWriter implements CacheWriter<String, String> {

        ConcurrentHashMap<String, String> database = new ConcurrentHashMap<>();

        @Override
        public void write(Cache.Entry<? extends String, ? extends String> entry) throws CacheWriterException {
            database.put(entry.getKey(), entry.getValue());
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends String, ? extends String>> entries) throws CacheWriterException {
            entries.forEach(this::write);
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            database.remove(key);
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            keys.forEach(this::delete);
        }

    }

    @Test
    public void testWriteThrough() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MockCacheWriter external = new MockCacheWriter();
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(external))
                    .setWriteThrough(true);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
            String key = "key";
            cache.put(key, "some_datum");

            {
                String result = external.database.get(key);
                assertEquals("some_datum", result);
            }

            assertEquals("some_datum", cache.get(key));
            cache.remove(key);
            {
                String result = external.database.get(key);
                assertNull(result);
            }

            cache.putIfAbsent(key, "datum2");
            {
                String result = external.database.get(key);
                assertEquals("datum2", result);
            }

            cache.replace(key, "datum3");
            {
                String result = external.database.get(key);
                assertEquals("datum3", result);
            }

            cache.replace(key, "datum_no", "datum4");
            {
                String result = external.database.get(key);
                assertEquals("datum3", result);
            }

            cache.replace(key, "datum3", "datum4");
            {
                String result = external.database.get(key);
                assertEquals("datum4", result);
            }

            Map<String, String> newValues = new HashMap<>();
            newValues.put("a", "a_value");
            newValues.put("b", "b_value");
            newValues.put("c", "c_value");
            cache.putAll(newValues);

            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertEquals(entry.getValue(), external.database.get(entry.getKey()));
            }

            cache.removeAll(newValues.keySet());

            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertNull(external.database.get(entry.getKey()));
            }

            cache.putAll(newValues);
            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertEquals(entry.getValue(), external.database.get(entry.getKey()));
            }
            cache.removeAll();
            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertNull(external.database.get(entry.getKey()));
            }
            cache.putAll(newValues);
            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertEquals(entry.getValue(), external.database.get(entry.getKey()));
            }
            // clear does not call the configured CacheWriter
            cache.clear();
            for (Map.Entry<String, String> entry : newValues.entrySet()) {
                assertEquals(entry.getValue(), external.database.get(entry.getKey()));
            }
        }
    }

    @Test
    public void testNoWriteThrough() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MockCacheWriter external = new MockCacheWriter();
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(external))
                    .setWriteThrough(false);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
            String key = "key";
            cache.put(key, "some_datum");

            String result = external.database.get(key);
            assertNull(result);
        }
    }

}
