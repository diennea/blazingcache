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
import java.util.Iterator;
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
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;

/**
 * Examples taken from JSR107 documentation
 *
 * @author enrico.olivelli
 */
public class IteratorTest {

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
        }    }

}
