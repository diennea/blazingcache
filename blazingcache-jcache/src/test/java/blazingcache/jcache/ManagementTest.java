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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.spi.CachingProvider;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class ManagementTest {

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
    public void testStatistics() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setStatisticsEnabled(true)
                    .setManagementEnabled(true);
            BlazingCacheCache<String, String> cache = cacheManager.createCache("simpleCache", config).unwrap(BlazingCacheCache.class);
            BlazingCacheStatisticsMXBean<String, String> stats = cache.getStatisticsMXBean();
            cache.put("1", "test");
            cache.get("1");
            assertEquals(1L, stats.getCachePuts());
            assertEquals(1L, stats.getCacheGets());
            assertEquals(1L, stats.getCacheHits());
            cache.get("2");
            assertEquals(1L, stats.getCacheMisses());
            stats.clear();
            assertEquals(0L, stats.getCachePuts());
            assertEquals(0L, stats.getCacheGets());
            assertEquals(0L, stats.getCacheHits());
            assertEquals(0L, stats.getCacheMisses());
        }
    }

    @Test
    public void testCacheStatisticsInvokeEntryProcessorRemove() throws Exception {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Long, String> config
                    = new MutableConfiguration<Long, String>()
                    .setTypes(Long.class, String.class)
                    .setStatisticsEnabled(true)
                    .setManagementEnabled(true);
            BlazingCacheCache<Long, String> cache = cacheManager.createCache("simpleCache", config).unwrap(BlazingCacheCache.class);

            cache.put(1l, "Sooty");
            String result = cache.invoke(1l, new RemoveEntryProcessor<Long, String, String>(true));
            assertEquals(result, "Sooty");
            assertEquals((int) 1, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) 1, (int) cache.getStatisticsMXBean().getCachePuts());
            assertEquals((int) 1, (int) cache.getStatisticsMXBean().getCacheRemovals());
        }
    }

    @Test
    public void testGetAndReplace() throws Exception {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Long, String> config
                    = new MutableConfiguration<Long, String>()
                    .setTypes(Long.class, String.class)
                    .setStatisticsEnabled(true)
                    .setManagementEnabled(true);
            BlazingCacheCache<Long, String> cache = cacheManager.createCache("simpleCache", config).unwrap(BlazingCacheCache.class);
            long hitCount = 0;
            long missCount = 0;
            long putCount = 0;

            String result = cache.getAndReplace(1L, "MissingNoReplace");
            missCount++;

            assertFalse(cache.containsKey(1L));
            assertEquals(null, result);
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());
            assertFalse(cache.containsKey(1L));

            cache.put(1l, "Sooty");
            putCount++;
            assertTrue(cache.containsKey(1L));
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());

            result = cache.getAndReplace(2L, "InvalidReplace");
            missCount++;
            assertEquals(null, result);
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());
            assertFalse(cache.containsKey(2L));

            result = cache.getAndReplace(1L, "Replaced");
            hitCount++;
            putCount++;
            assertEquals("Sooty", result);
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());
        }
    }

    @Test
    public void testPutIfAbsent() throws Exception {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Long, String> config
                    = new MutableConfiguration<Long, String>()
                    .setTypes(Long.class, String.class)
                    .setStatisticsEnabled(true)
                    .setManagementEnabled(true);
            BlazingCacheCache<Long, String> cache = cacheManager.createCache("simpleCache", config).unwrap(BlazingCacheCache.class);
            long hitCount = 0;
            long missCount = 0;
            long putCount = 0;

            boolean result = cache.putIfAbsent(1L, "succeeded");
            putCount++;
//            missCount++;
            assertTrue(result);
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());
            assertTrue(cache.containsKey(1L));

            result = cache.putIfAbsent(1L, "succeeded");
            assertFalse(result);
//            hitCount++;
            assertEquals((int) missCount, (int) cache.getStatisticsMXBean().getCacheMisses());
            assertEquals((int) hitCount, (int) cache.getStatisticsMXBean().getCacheHits());
            assertEquals((int) putCount, (int) cache.getStatisticsMXBean().getCachePuts());
        }
    }

    @Test
    public void testCacheStatistics() throws Exception {
        final float DELTA = 1.0f;
        final float DELTA_0 = 0.0f;
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<Long, String> config
                    = new MutableConfiguration<Long, String>()
                    .setTypes(Long.class, String.class)
                    .setStatisticsEnabled(true)
                    .setManagementEnabled(true);
            BlazingCacheCache<Long, String> cache = cacheManager.createCache("simpleCache", config).unwrap(BlazingCacheCache.class);
            cache.put(1l, "Sooty");
            assertEquals(0L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(1L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            Map<Long, String> entries = new HashMap<Long, String>();
            entries.put(2l, "Lucky");
            entries.put(3l, "Prince");
            cache.putAll(entries);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //Update. But we count these simply as puts for stats
            cache.put(1l, "Sooty");
            assertEquals(0L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(4L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.putAll(entries);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(0L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(6L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.getAndPut(4l, "Cody");
            assertEquals(0L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(1L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(100.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(7L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.getAndPut(4l, "Cody");
            assertEquals(1L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(1L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            String value = cache.get(1l);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(66.0f, (float) cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA);
            assertEquals(1L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(33.0f, (float) cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //now do a second miss
            value = cache.get(1234324324l);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //containsKey() should not affect statistics
            assertTrue(cache.containsKey(1l));
            assertFalse(cache.containsKey(1234324324l));
            assertEquals(2L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            assertTrue(cache.remove(1L));
            assertEquals(2L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(1L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //no update to cache removals as does not exist
            assertFalse(cache.remove(1L));
            assertEquals(2L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(8L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(1L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //should update removals as succeeded
            cache.put(1l, "Sooty");
            assertTrue(cache.remove(1L, "Sooty"));
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(60.0f, (float) cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA);
            assertEquals(2L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(40.0f, (float) cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA);
            assertEquals(9L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(2L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            //should not update removals as remove failed
            assertFalse(cache.remove(1L, "Sooty"));
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(9L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(2L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.clear();
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(9L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(2L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.removeAll();
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(9L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(2L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            entries.put(21L, "Trinity");
            cache.putAll(entries);
            cache.removeAll();
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(12L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(5L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.putAll(entries);
            entries.remove(21L);
            System.out.println("removing " + entries.keySet().size() + " entries");
            cache.removeAll(entries.keySet());
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(15L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(7L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

            cache.removeAll(entries.keySet());
            assertEquals(3L, cache.getStatisticsMXBean().getCacheHits());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheHitPercentage(), DELTA_0);
            assertEquals(3L, cache.getStatisticsMXBean().getCacheMisses());
            assertEquals(50.0f, cache.getStatisticsMXBean().getCacheMissPercentage(), DELTA_0);
            assertEquals(15L, cache.getStatisticsMXBean().getCachePuts());
            assertEquals(7L, cache.getStatisticsMXBean().getCacheRemovals());
            assertEquals(0L, cache.getStatisticsMXBean().getCacheEvictions());

        }
    }

    @Test
    public void loadAllWithReadThroughEnabledShouldCallGetExpiryForCreatedEntry() throws IOException, ExecutionException, InterruptedException {
        //establish and open a CacheLoaderServer to handle cache
        //cache loading requests from a CacheLoaderClient
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            // this cacheLoader just returns the key as the value.
            RecordingCacheLoader<Integer> recordingCacheLoader = new RecordingCacheLoader<>();

            CountingExpiryPolicy expiryPolicy = new CountingExpiryPolicy();

            MutableConfiguration<Integer, Integer> config = new MutableConfiguration<>();
            config.setExpiryPolicyFactory(FactoryBuilder.factoryOf(expiryPolicy));
            config.setCacheLoaderFactory(new FactoryBuilder.SingletonFactory<>(recordingCacheLoader));
            config.setReadThrough(true);

            Cache<Integer, Integer> cache = cacheManager.createCache("test-aaa", config);

            final Integer INITIAL_KEY = 123;
            final Integer MAX_KEY_VALUE = INITIAL_KEY + 4;

            // set half of the keys so half of loadAdd will be loaded
            Set<Integer> keys = new HashSet<>();
            for (int key = INITIAL_KEY; key <= MAX_KEY_VALUE; key++) {
                keys.add(key);
            }

            // verify read-through of getValue of non-existent entries
            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(keys, false, future);

            //wait for the load to complete
            future.get();

            assertThat(future.isDone(), is(true));
            assertThat(recordingCacheLoader.getLoadCount(), is(keys.size()));

            assertTrue(expiryPolicy.getCreationCount() >= keys.size());
            assertThat(expiryPolicy.getAccessCount(), is(0));
            assertThat(expiryPolicy.getUpdatedCount(), is(0));
            expiryPolicy.resetCount();

            for (Integer key : keys) {
                assertThat(recordingCacheLoader.hasLoaded(key), is(true));
                assertThat(cache.get(key), is(equalTo(key)));
            }
            assertTrue(expiryPolicy.getAccessCount() >= keys.size());
            expiryPolicy.resetCount();

            // verify read-through of getValue for existing entries AND replaceExistingValues is true.
            final boolean REPLACE_EXISTING_VALUES = true;
            future = new CompletionListenerFuture();
            cache.loadAll(keys, REPLACE_EXISTING_VALUES, future);

            //wait for the load to complete
            future.get();

            assertThat(future.isDone(), is(true));
            assertThat(recordingCacheLoader.getLoadCount(), is(keys.size() * 2));

            assertThat(expiryPolicy.getCreationCount(), is(0));
            assertThat(expiryPolicy.getAccessCount(), is(0));
            assertTrue(expiryPolicy.getUpdatedCount() >= keys.size());
            expiryPolicy.resetCount();

            for (Integer key : keys) {
                assertThat(recordingCacheLoader.hasLoaded(key), is(true));
                assertThat(cache.get(key), is(equalTo(key)));
            }
        }

    }
}
