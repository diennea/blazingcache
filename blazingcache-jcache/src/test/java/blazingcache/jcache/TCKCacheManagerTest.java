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

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import static javax.cache.event.EventType.CREATED;
import static javax.cache.event.EventType.REMOVED;
import static javax.cache.event.EventType.UPDATED;
import javax.cache.spi.CachingProvider;
import static junit.framework.TestCase.assertNotNull;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class TCKCacheManagerTest {

    @Test
    public void getCacheManager_nonNullProperties() {
        CachingProvider provider = Caching.getCachingProvider();
        Properties properties = new Properties();

        assertSame(provider.getCacheManager(),
                provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader(), new Properties()));

        try (CacheManager manager = provider.getCacheManager();) {
            assertEquals(properties, manager.getProperties());
        }
    }

    @Test(expected = NullPointerException.class)
    public void getCacheNullValueClass() {
        String name = "c1";
        CacheManager manager = Caching.getCachingProvider().getCacheManager();
        manager.createCache(name, new MutableConfiguration().setTypes(Long.class, String.class));
        try {
            Caching.getCache(name, Long.class, null);
        } finally {
            manager.destroyCache(name);
        }
    }

    @Test
    public void testReuseCacheManagerGetCache() throws Exception {
        CachingProvider provider = Caching.getCachingProvider();
        URI uri = provider.getDefaultURI();

        CacheManager cacheManager = provider.getCacheManager(uri, provider.getDefaultClassLoader());
        assertFalse(cacheManager.isClosed());
        cacheManager.close();
        assertTrue(cacheManager.isClosed());

        try {
            cacheManager.getCache("nonExistent", null, null);
            fail();
        } catch (IllegalStateException e) {
            //expected
        }

        CacheManager otherCacheManager = provider.getCacheManager(uri, provider.getDefaultClassLoader());
        assertFalse(otherCacheManager.isClosed());

        assertNotSame(cacheManager, otherCacheManager);
    }

    private CacheManager getCacheManager() {
        return Caching.getCachingProvider().getCacheManager();
    }

    @After
    public void closeall() {
        Caching.getCachingProvider().close();
    }

    @Test
    public void createCache_StatusOK() {
        String name = "c1";
        getCacheManager().createCache(name, new MutableConfiguration());
        Cache cache = getCacheManager().getCache(name);
        assertNotNull(cache);
        assertEquals(name, cache.getName());
    }

    @Test
    public void getOrCreateCache_Different() {
        String name1 = "c1";
        CacheManager cacheManager = getCacheManager();
        cacheManager.createCache(name1, new MutableConfiguration());
        Cache cache1 = cacheManager.getCache(name1);

        String name2 = "c2";
        cacheManager.createCache(name2, new MutableConfiguration());
        Cache cache2 = cacheManager.getCache(name2);

        assertEquals(cache1, cacheManager.getCache(name1));
        assertEquals(cache2, cacheManager.getCache(name2));
    }

    @Test
    public void createCache_Different() {
        String name1 = "c1";
        CacheManager cacheManager = getCacheManager();
        cacheManager.createCache(name1, new MutableConfiguration());
        Cache cache1 = cacheManager.getCache(name1);

        String name2 = "c2";
        cacheManager.createCache(name2, new MutableConfiguration());
        Cache cache2 = cacheManager.getCache(name2);

        assertEquals(cache1, cacheManager.getCache(name1));
        assertEquals(cache2, cacheManager.getCache(name2));
    }

    @Test
    public void createCacheSameName() {
        CacheManager cacheManager = getCacheManager();
        String name1 = "c1";
        cacheManager.createCache(name1, new MutableConfiguration());
        Cache cache1 = cacheManager.getCache(name1);
        assertEquals(cache1, cacheManager.getCache(name1));
        ensureOpen(cache1);

        try {
            cacheManager.createCache(name1, new MutableConfiguration());
        } catch (CacheException e) {
            //expected
        }
        Cache cache2 = cacheManager.getCache(name1);
    }

    @Test
    public void removeCache_Null() {
        CacheManager cacheManager = getCacheManager();
        try {
            cacheManager.destroyCache(null);
            fail("should have thrown an exception - cache name null");
        } catch (NullPointerException e) {
            //good
        }
    }

    private void ensureOpen(Cache cache) {
        if (cache.isClosed()) {
            fail();
        }
    }

    private void ensureClosed(Cache cache) {
        if (!cache.isClosed()) {
            fail();
        }
    }

    @Test
    public void removeCache_There() {
        CacheManager cacheManager = getCacheManager();
        String name1 = "c1";
        cacheManager.createCache(name1, new MutableConfiguration());
        cacheManager.destroyCache(name1);
        assertFalse(cacheManager.getCacheNames().iterator().hasNext());
    }

    @Test
    public void removeCache_CacheStopped() {
        CacheManager cacheManager = getCacheManager();
        String name1 = "c1";
        cacheManager.createCache(name1, new MutableConfiguration());
        Cache cache1 = cacheManager.getCache(name1);
        cacheManager.destroyCache(name1);
        ensureClosed(cache1);
    }

    @Test(expected = ClassCastException.class)
    public void getIncorrectCacheType() {
        CacheManager cacheManager = getCacheManager();

        MutableConfiguration<String, Long> config = new MutableConfiguration<String, Long>().setTypes(String.class, Long.class);

        cacheManager.createCache("typed-cache", config);

        Cache<Long, String> cache = cacheManager.getCache("typed-cache", Long.class, String.class);
    }

    @Test(expected = ClassCastException.class)
    public void getIncorrectCacheValueType() {
        CacheManager cacheManager = getCacheManager();

        MutableConfiguration<String, Long> config = new MutableConfiguration<String, Long>().setTypes(String.class, Long.class);

        cacheManager.createCache("typed-cache", config);

        Cache<String, String> cache = cacheManager.getCache("typed-cache", String.class, String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getUnsafeTypedCacheRequest() {
        CacheManager cacheManager = getCacheManager();

        MutableConfiguration<String, Long> config = new MutableConfiguration<String, Long>().setTypes(String.class, Long.class);

        cacheManager.createCache("typed-cache", config);

        Cache cache = cacheManager.getCache("typed-cache");
    }

    @Test
    public void getAndReplace_Missing() {
        Cache cache = getCacheManager().createCache("test", new MutableConfiguration<Object, Object>());
        Long key = System.currentTimeMillis();
        assertNull(cache.getAndReplace(key, ""));
        assertFalse(cache.containsKey(key));
    }

    @Test(expected = NullPointerException.class)
    public void getNullTypeCacheRequest() {
        CacheManager cacheManager = getCacheManager();

        MutableConfiguration config = new MutableConfiguration();

        cacheManager.createCache("untyped-cache", config);

        Cache cache = cacheManager.getCache("untyped-cache", null, null);
    }

    @Test
    public void removeCache_NotThere() {
        CacheManager cacheManager = getCacheManager();
        cacheManager.destroyCache("c1");
    }

    @Test
    public void removeCache_Stopped() {
        CacheManager cacheManager = getCacheManager();
        cacheManager.close();
        try {
            cacheManager.destroyCache("c1");
            fail();
        } catch (IllegalStateException e) {
            //ok
        }
    }

    @Test
    public void close_cachesClosed() {
        CacheManager cacheManager = getCacheManager();

        cacheManager.createCache("c1", new MutableConfiguration());
        Cache cache1 = cacheManager.getCache("c1");
        cacheManager.createCache("c2", new MutableConfiguration());
        Cache cache2 = cacheManager.getCache("c2");

        cacheManager.close();

        ensureClosed(cache1);
        ensureClosed(cache2);
    }

}
