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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class IteratorTest {

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
    public void testIterator() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class);
            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
            Map<String, String> expected = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                expected.put("key" + i, "value" + i);
            }
            for (Iterator<Cache.Entry<String, String>> it = cache.iterator(); it.hasNext();) {
                Cache.Entry<String, String> entry = it.next();
                BlazingCacheEntry<String, String> unwrapped = entry.unwrap(BlazingCacheEntry.class);
                assertNotNull(unwrapped);
                assertEquals(entry.getValue(), expected.get(entry.getKey()));
            }
            for (Iterator<Cache.Entry<String, String>> it = cache.iterator(); it.hasNext();) {
                Cache.Entry<String, String> entry = it.next();
                BlazingCacheEntry<String, String> unwrapped = entry.unwrap(BlazingCacheEntry.class);
                assertNotNull(unwrapped);
                assertEquals(entry.getValue(), expected.get(entry.getKey()));
                it.remove();
            }
            for (String key : expected.keySet()) {
                assertNull(cache.get(key));
            }
            // empty iterator
            for (Iterator<Cache.Entry<String, String>> it = cache.iterator(); it.hasNext();) {
                fail();
            }
            try {
                Iterator<Cache.Entry<String, String>> it = cache.iterator();
                it.next();
                fail();
            } catch (NoSuchElementException expectedError) {
            }
            try {
                Iterator<Cache.Entry<String, String>> it = cache.iterator();
                it.remove();
                fail();
            } catch (IllegalStateException expectedError) {
            }
        }
    }

    @Test
    public void iterator() {
        LinkedHashMap<Long, String> data = new LinkedHashMap<Long, String>();
        data.put(1L, "one");
        data.put(2L, "two");

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Cache<Long, String> cache = cacheManager.createCache("test", new MutableConfiguration().setTypes(Long.class, String.class));
            cache.putAll(data);
            Iterator<Cache.Entry<Long, String>> iterator = cache.iterator();
            while (iterator.hasNext()) {
                Cache.Entry<Long, String> next = iterator.next();
                assertEquals(next.getValue(), data.get(next.getKey()));
                iterator.remove();
                data.remove(next.getKey());
            }
            assertTrue(data.isEmpty());
        }
    }

    @Test
    public void testIterateAndRemove() throws Exception {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Cache<Long, String> cache = cacheManager.createCache("test", new MutableConfiguration().setTypes(Long.class, String.class));
            BlazingCacheCache<Long, String> blazingCache = cache.unwrap(BlazingCacheCache.class);
            for (long i = 0; i < 100L; i++) {
                String word = "";
                word = word + " " + "Trinity";
                cache.put(i, word);
            }

            Iterator<Cache.Entry<Long, String>> iterator = cache.iterator();

            while (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
            }

            assertEquals(100, (int) blazingCache.getStatisticsMXBean().getCacheHits());
            assertEquals(100, (int) blazingCache.getStatisticsMXBean().getCacheHitPercentage());
            assertEquals(0, (int) blazingCache.getStatisticsMXBean().getCacheMisses());
            assertEquals(0, (int) blazingCache.getStatisticsMXBean().getCacheMissPercentage());
            assertEquals(100, (int) blazingCache.getStatisticsMXBean().getCacheGets());
            assertEquals(100, (int) blazingCache.getStatisticsMXBean().getCachePuts());
            assertEquals(100, (int) blazingCache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0, (int) blazingCache.getStatisticsMXBean().getCacheEvictions());
//            assertTrue(, (int) blazingCache.getStatisticsMXBean().getCache
//            assertThat((Float) lookupManagementAttribute(cache, CacheStatistics, "AveragePutTime"), greaterThanOrEqualTo(0f));
//            assertThat((Float) lookupManagementAttribute(cache, CacheStatistics, "AverageRemoveTime"), greaterThanOrEqualTo(0f));
        }
    }

    @Test
    public void testConditionalReplace() throws Exception {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            Cache<Long, String> cache = cacheManager.createCache("test", new MutableConfiguration().setTypes(Long.class, String.class));
            BlazingCacheCache<Long, String> blazingCache = cache.unwrap(BlazingCacheCache.class);
            long hitCount = 0;
            long missCount = 0;
            long putCount = 0;

            boolean result = cache.replace(1L, "MissingNoReplace", "NewValue");
            missCount++;
            assertFalse(result);
            assertEquals((int) missCount, (int) blazingCache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) blazingCache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) blazingCache.getStatisticsMXBean().getCachePuts());
            assertFalse(cache.containsKey(1L));

            cache.put(1l, "Sooty");
            putCount++;
            assertEquals((int) putCount, (int) blazingCache.getStatisticsMXBean().getCachePuts());

            assertTrue(cache.containsKey(1L));
            result = cache.replace(1L, "Sooty", "Replaced");
            hitCount++;
            putCount++;
            assertTrue(result);
            assertEquals((int) missCount, (int) blazingCache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) blazingCache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) blazingCache.getStatisticsMXBean().getCachePuts());

            result = cache.replace(1L, "Sooty", "InvalidReplace");
            hitCount++;
            assertFalse(result);
            assertEquals((int) missCount, (int) blazingCache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) blazingCache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) blazingCache.getStatisticsMXBean().getCachePuts());
        }
    }

}
