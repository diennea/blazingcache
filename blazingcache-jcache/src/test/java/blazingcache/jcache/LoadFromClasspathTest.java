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

import java.util.Properties;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import org.junit.Test;

import org.junit.After;
import static org.junit.Assert.assertEquals;

/** 
 *
 * @author enrico.olivelli
 */
public class LoadFromClasspathTest {

    @After
    public void clear() {
        Caching.getCachingProvider().close();
    }

    @Test
    public void testLoadCustomConfigFile() {
        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        p.setProperty("configfile", "custom_config.properties");
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            assertEquals("1000000", cacheManager.getProperties().get("blazingcache.jcache.maxmemory"));
            assertEquals("test", cacheManager.getProperties().get("otherproperty"));
        }
    }

}
