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

import java.util.Arrays;
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
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.spi.CachingProvider;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class CacheLoaderTest {

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

    public static class MockCacheLoader implements CacheLoader<String, Object> {

        AtomicInteger loadCount = new AtomicInteger();
        Set<String> loaded = new HashSet<>();
        
        public int getLoadCount() {
            return loadCount.get();
        }
        
        public boolean hasLoaded(String key) {
            return loaded.contains(key);
        }
        
        @Override
        public Object load(String key) throws CacheLoaderException {
            loadCount.incrementAndGet();
            loaded.add(key);
            return "LOADED_" + key;
        }

        @Override
        public Map<String, Object> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            System.out.println("loadAll: "+keys);
            Map<String, Object> res = new HashMap<>();
            for (String key : keys) {
                res.put(key, load(key));
            }
            return res;
        }

    }

    @Test
    public void shouldNotLoadWithNullKeyUsingLoadAll() throws Exception {

        HashSet<String> keys = new HashSet<>();
        keys.add(null);

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(true);
            Cache<String, String> cache = cacheManager.createCache("test", config);
            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(keys, false, future);

            fail("Expected a NullPointerException");
        } catch (NullPointerException e) {
            //SKIP: expected
        } finally {
//            assertThat(cacheLoader.getLoadCount(), is(0));
        }
    }

    @Test
    public void testReadThrough() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(true);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);

            String key = "key";
            String result = cache.get(key);
            assertEquals("LOADED_" + key, result);
        }
    }

    @Test
    public void testReadThroughGetAll() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(true);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);

            String key = "key";
            Map<String, String> result = cache.getAll(new HashSet<>(Arrays.asList(key)));
            assertEquals("LOADED_" + key, result.get(key));
            assertEquals("LOADED_" + key, cache.get(key));
        }
    }

    @Test
    public void testNoReadThrough() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(false);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);

            String key = "key";
            String result = cache.get(key);
            assertNull(result);
        }
    }

    @Test
    public void testLoadallReplaceExisting() throws Exception {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(false);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
            cache.put("one", "to_be_replaced");
            Set<String> keys = new HashSet<>();
            keys.add("one");
            keys.add("two");
            keys.add("three");
            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(keys, true, future);
            future.get();

            for (String key : keys) {
                String result = cache.get(key);
                assertEquals("LOADED_" + key, result);
            }
        }
    }

    @Test
    public void testLoadallNoReplaceExisting() throws Exception {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, String> config
                    = new MutableConfiguration<String, String>()
                    .setTypes(String.class, String.class)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                    .setReadThrough(false);

            Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
            cache.put("one", "not_to_be_replaced");
            Set<String> keys = new HashSet<>();
            keys.add("one");
            keys.add("two");
            keys.add("three");
            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(keys, false, future);
            future.get();

            for (String key : keys) {
                String result = cache.get(key);
                if (key.equals("one")) {
                    assertEquals("not_to_be_replaced", result);
                } else {
                    assertEquals("LOADED_" + key, result);
                }
            }
        }
    }

    @Test
    public void shouldLoadUsingGetAll() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        MockCacheLoader cacheLoader = new MockCacheLoader();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, Object> config
                    = new MutableConfiguration<String, Object>()
                    .setTypes(String.class, Object.class)
                    .setCacheLoaderFactory(new FactoryBuilder.SingletonFactory<>(cacheLoader))
                    .setReadThrough(true);

            Cache<String, Object> cache = cacheManager.createCache("simpleCache", config);

            //construct a set of keys
            HashSet<String> keys = new HashSet<String>();
            keys.add("gudday");
            keys.add("hello");
            keys.add("howdy");
            keys.add("bonjour");

            //get the keys
            Map<String, Object> map = cache.getAll(keys);

            //assert that the map content is as expected
            assertThat(map.size(), is(keys.size()));

            for (String key : keys) {
                assertThat(map.containsKey(key), is(true));
                assertThat(map.get(key), is("LOADED_"+key));
                assertThat(cache.get(key), is("LOADED_"+key));
            }

            //assert that the loader state is as expected
            assertThat(cacheLoader.getLoadCount(), is(keys.size()));

            for (String key : keys) {
                assertThat(cacheLoader.hasLoaded(key), is(true));
            }

            //attempting to load the same keys should not result in another load
            cache.getAll(keys);
            assertThat(cacheLoader.getLoadCount(), is(keys.size()));
        }
    }
}
