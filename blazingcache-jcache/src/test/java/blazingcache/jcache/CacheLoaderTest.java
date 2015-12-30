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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class CacheLoaderTest {

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

        @Override
        public Object load(String key) throws CacheLoaderException {
            return "LOADED_" + key;
        }

        @Override
        public Map<String, Object> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            Map<String, Object> res = new HashMap<>();
            for (String key : keys) {
                res.put(key, load(key));
            }
            return res;
        }

    }

    @Test
    public void testReadThrough() {
        //resolve a cache manager
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p);
//configure the cache
        MutableConfiguration<String, String> config
                = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class)
                .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                .setReadThrough(true);

//create the cache
        Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
//cache operations
        String key = "key";
        String result = cache.get(key);
        assertEquals("LOADED_" + key, result);
    }

    @Test
    public void testNoReadThrough() {
        //resolve a cache manager
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p);
//configure the cache
        MutableConfiguration<String, String> config
                = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class)
                .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(MockCacheLoader.class))
                .setReadThrough(false);

//create the cache
        Cache<String, String> cache = cacheManager.createCache("simpleCache", config);
//cache operations
        String key = "key";
        String result = cache.get(key);
        assertNull(result);
    }

    @Test
    public void testLoadallReplaceExisting() throws Exception {
        //resolve a cache manager
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p);
//configure the cache
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

    @Test
    public void testLoadallNoReplaceExisting() throws Exception {
        //resolve a cache manager
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p);
//configure the cache
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
