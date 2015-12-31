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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.spi.CachingProvider;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.is;

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

    @Test
    public void getCaches_MutateCacheManager() {
        CacheManager cacheManager = getCacheManager();

        String removeName = "c2";
        ArrayList<String> cacheNames1 = new ArrayList<String>();
        cacheManager.createCache("c1", new MutableConfiguration());
        Cache c1 = cacheManager.getCache("c1");
        cacheNames1.add(c1.getName());
        cacheManager.createCache(removeName, new MutableConfiguration());
        cacheManager.createCache("c3", new MutableConfiguration());
        Cache c3 = cacheManager.getCache("c3");
        cacheNames1.add(c3.getName());

        Iterable<String> cacheNames;
        int size;

        cacheNames = cacheManager.getCacheNames();
        size = 0;
        for (String cacheName : cacheNames) {
            size++;
        }
        assertEquals(3, size);
        cacheManager.destroyCache(removeName);
        size = 0;
        for (String cacheName : cacheNames) {
            size++;
        }
        assertEquals(3, size);

        cacheNames = cacheManager.getCacheNames();
        size = 0;
        for (String cacheName : cacheNames) {
            size++;
        }
        assertEquals(2, size);
        checkCollections(cacheNames1, cacheNames);
    }

    private <T> void checkCollections(Collection<T> collection1, Iterable<?> iterable2) {
        ArrayList<Object> collection2 = new ArrayList<Object>();
        for (Object element : iterable2) {
            assertTrue(collection1.contains(element));
            collection2.add(element);
        }
        assertEquals(collection1.size(), collection2.size());
        for (T element : collection1) {
            assertTrue(collection2.contains(element));
        }
    }

    @Test
    public void createCache_Same() {
        String name = "c1";
        CacheManager cacheManager = getCacheManager();
        try {
            cacheManager.createCache(name, new MutableConfiguration());
            Cache cache1 = cacheManager.getCache(name);
            cacheManager.createCache(name, new MutableConfiguration());
            Cache cache2 = cacheManager.getCache(name);
            fail();
        } catch (CacheException exception) {
            //expected
        }
    }

    @Test
    public void shouldNotLoadWithNullKeyUsingLoadAll() throws Exception {

        HashSet<String> keys = new HashSet<>();
        keys.add(null);

        try {
            CacheManager cacheManager = getCacheManager();
            Cache<String,String > cache = cacheManager.createCache("test", new MutableConfiguration<String, String>());
            CompletionListenerFuture future = new CompletionListenerFuture();
            cache.loadAll(keys, false, future);

            fail("Expected a NullPointerException");
        } catch (NullPointerException e) {
            //SKIP: expected
        } finally {
//            assertThat(cacheLoader.getLoadCount(), is(0));
        }
    }

}
