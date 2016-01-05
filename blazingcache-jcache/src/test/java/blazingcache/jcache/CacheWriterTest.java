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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
            entries.clear();
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            System.out.println("delete:" + key);
            database.remove(key);
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            System.out.println("deleteAll:" + keys);
            keys.forEach(this::delete);
            keys.clear();
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

    @Test
    public void shouldWriteThroughUsingInvokeAll_setValue_RemoveEntry() {
        RecordingCacheWriter<Integer, String> cacheWriter = new RecordingCacheWriter<>();
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);
            final String VALUE_PREFIX = "value_";
            final int NUM_KEYS = 10;

            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            Set<Integer> keys = new HashSet<>();
            for (int key = 1; key <= NUM_KEYS; key++) {
                keys.add(key);
                cache.put(key, VALUE_PREFIX + key);
            }

            assertEquals(NUM_KEYS, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            cache.invokeAll(keys, new RemoveEntryProcessor<Integer, String, Object>(true));
            assertEquals(NUM_KEYS, cacheWriter.getWriteCount());
            assertEquals(NUM_KEYS, cacheWriter.getDeleteCount());

            for (Integer key : keys) {
                assertFalse(cacheWriter.hasWritten(key));
                assertEquals(null, cacheWriter.get(key));
                assertEquals(null, cache.get(key));
            }
        }
    }

    @Test
    public void shouldWriteThroughRemoveAllSpecific_partialSuccess() {
        BatchPartialSuccessRecordingClassWriter<Integer, String> cacheWriter = new BatchPartialSuccessRecordingClassWriter<>(100, 3);
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);
            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            HashMap<Integer, String> entriesAdded = new HashMap<>();
            entriesAdded.put(1, "Gudday World");
            entriesAdded.put(2, "Bonjour World");
            entriesAdded.put(3, "Hello World");
            entriesAdded.put(4, "Hola World");
            entriesAdded.put(5, "Ciao World");

            cache.putAll(entriesAdded);

            assertEquals(5, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            for (Integer key : entriesAdded.keySet()) {
                assertTrue(cacheWriter.hasWritten(key));
                assertEquals(entriesAdded.get(key), cacheWriter.get(key));
                assertTrue(cache.containsKey(key));
                assertEquals(entriesAdded.get(key), cache.get(key));
            }

            HashSet<Integer> keysToRemove = new HashSet<>();
            keysToRemove.add(1);
            keysToRemove.add(2);
            keysToRemove.add(3);
            keysToRemove.add(4);

            try {
                cache.removeAll(keysToRemove);
                assertTrue("expected CacheWriterException to be thrown for BatchPartialSuccessRecordingClassWriter", false);
            } catch (CacheWriterException ce) {
                ce.printStackTrace();
                // ignore expected exception
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (Integer key : entriesAdded.keySet()) {
                if (cacheWriter.hasWritten(key)) {
                    System.out.println("I want " + key + " present");
                    assertTrue(cache.containsKey(key));
                    assertEquals(entriesAdded.get(key), cacheWriter.get(key));
                    assertEquals(entriesAdded.get(key), cache.get(key));
                    numFailure++;
                } else {
                    System.out.println("I want " + key + " not present");
                    assertFalse(cache.containsKey(key));
                    numSuccess++;
                }
                assertEquals(cache.get(key), cacheWriter.get(key));
            }

            assertEquals(numSuccess + numFailure, entriesAdded.size());
            assertEquals(5, cacheWriter.getWriteCount());
            assertEquals(numSuccess, cacheWriter.getDeleteCount());
        }
    }

    @Test
    public void shouldWriteThroughRemoveAll_partialSuccess() {
        BatchPartialSuccessRecordingClassWriter<Integer, String> cacheWriter = new BatchPartialSuccessRecordingClassWriter<>(100, 3);
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);

            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            HashMap<Integer, String> map = new HashMap<>();
            map.put(1, "Gudday World");
            map.put(2, "Bonjour World");
            map.put(3, "Hello World");

            cache.putAll(map);

            assertEquals(3, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            for (Integer key : map.keySet()) {
                assertTrue(cacheWriter.hasWritten(key));
                assertEquals(map.get(key), cacheWriter.get(key));
                assertTrue(cache.containsKey(key));
                assertEquals(map.get(key), cache.get(key));
            }

            try {
                cache.removeAll();
                assertTrue("expected CacheWriterException to be thrown for BatchPartialSuccessRecordingClassWriter", false);
            } catch (CacheWriterException cwe) {

                // ignore expected exception
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (Integer key : map.keySet()) {
                if (cacheWriter.hasWritten(key)) {
                    assertTrue(cache.containsKey(key));
                    assertEquals(map.get(key), cacheWriter.get(key));
                    assertEquals(map.get(key), cache.get(key));
                    numFailure++;
                } else {
                    assertFalse(cache.containsKey(key));
                    numSuccess++;
                }

                assertEquals(cache.get(key), cacheWriter.get(key));
            }

            assertEquals(numSuccess + numFailure, map.size());
            assertEquals(3, cacheWriter.getWriteCount());
            assertEquals(numSuccess, cacheWriter.getDeleteCount());
        }
    }

    @Test
    public void shouldWriteThroughRemoveAll_partialSuccessAndListener() {
        BatchPartialSuccessRecordingClassWriter<Integer, String> cacheWriter = new BatchPartialSuccessRecordingClassWriter<>(100, 3);
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        CacheListenersTest.MyCacheEntryListener<Integer, String> listener = new CacheListenersTest.MyCacheEntryListener<>();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(FactoryBuilder.factoryOf(listener), null, true, true))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);

            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            HashMap<Integer, String> map = new HashMap<>();
            map.put(1, "Gudday World");
            map.put(2, "Bonjour World");
            map.put(3, "Hello World");

            cache.putAll(map);

            assertEquals(3, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            for (Integer key : map.keySet()) {
                assertTrue(cacheWriter.hasWritten(key));
                assertEquals(map.get(key), cacheWriter.get(key));
                assertTrue(cache.containsKey(key));
                assertEquals(map.get(key), cache.get(key));
            }

            try {
                cache.removeAll();
                assertTrue("expected CacheWriterException to be thrown for BatchPartialSuccessRecordingClassWriter", false);
            } catch (CacheWriterException cwe) {

                // ignore expected exception
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (Integer key : map.keySet()) {
                if (cacheWriter.hasWritten(key)) {
                    assertTrue(cache.containsKey(key));
                    assertEquals(map.get(key), cacheWriter.get(key));
                    assertEquals(map.get(key), cache.get(key));
                    numFailure++;
                } else {
                    assertFalse(cache.containsKey(key));
                    numSuccess++;
                }

                assertEquals(cache.get(key), cacheWriter.get(key));
            }

            assertEquals(numSuccess + numFailure, map.size());
            assertEquals(3, cacheWriter.getWriteCount());
            assertEquals(numSuccess, cacheWriter.getDeleteCount());
            assertEquals(numSuccess, listener.removed.get());
        }
    }

    @Test
    public void shouldWriteThoughUsingPutAll_partialSuccess() {
        BatchPartialSuccessRecordingClassWriter<Integer, String> cacheWriter = new BatchPartialSuccessRecordingClassWriter<>(3, 100);
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);
            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            HashMap<Integer, String> map = new HashMap<>();
            map.put(1, "Gudday World");
            map.put(2, "Bonjour World");
            map.put(3, "Hello World");
            map.put(4, "Ciao World");

            try {
                cache.putAll(map);
                assertTrue("expected CacheWriterException to be thrown for BatchPartialSuccessRecordingClassWriter", false);
            } catch (CacheWriterException cwe) {

                // ignore expected exception
            }

            int numSuccess = 0;
            int numFailure = 0;
            for (Integer key : map.keySet()) {
                if (cacheWriter.hasWritten(key)) {
                    assertTrue(cache.containsKey(key));
                    assertEquals(map.get(key), cacheWriter.get(key));
                    assertEquals(map.get(key), cache.get(key));
                    numSuccess++;
                } else {
                    assertFalse(cache.containsKey(key));
                    assertFalse(cacheWriter.hasWritten(key));
                    assertEquals(cache.get(key), cacheWriter.get(key));
                    numFailure++;
                }

                assertEquals(cache.get(key), cacheWriter.get(key));
            }

            assertEquals(numSuccess + numFailure, map.size());
            assertEquals(numSuccess, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());
        }
    }

    @Test
    public void shouldWriteThroughUsingInvoke_setValue_CreateEntryThenRemove() {
        RecordingCacheWriter<Integer, String> cacheWriter = new RecordingCacheWriter<>();
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Integer, String> config
                    = new MutableConfiguration<Integer, String>()
                    .setTypes(Integer.class, String.class)
                    .setCacheWriterFactory(new FactoryBuilder.SingletonFactory<>(cacheWriter))
                    .setWriteThrough(true);

            Cache<Integer, String> cache = cacheManager.createCache("simpleCache", config);
            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());

            EntryProcessor processors[]
                    = new EntryProcessor[]{
                        new AssertNotPresentEntryProcessor(null),
                        new SetEntryProcessor<Integer, String>("Gudday World"),
                        new RemoveEntryProcessor<Integer, String, Object>(true)
                    };
            cache.invoke(1, new CombineEntryProcessor<Integer, String>(processors));
            assertEquals(0, cacheWriter.getWriteCount());
            assertEquals(0, cacheWriter.getDeleteCount());
            assertTrue(!cacheWriter.hasWritten(1));
            assertTrue(cache.get(1) == null);
            assertFalse(cache.containsKey(1));
        }
    }
}
