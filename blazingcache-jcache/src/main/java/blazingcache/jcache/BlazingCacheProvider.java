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
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.CacheManager;
import javax.cache.configuration.OptionalFeature;
import javax.cache.spi.CachingProvider;

/**
 * Implementation of JSR 107
 *
 * @author enrico.olivelli
 */
public class BlazingCacheProvider implements CachingProvider {

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader, Properties properties) {
        String key = uri.toASCIIString();
        BlazingCacheManager bl = managers.get(key);
        if (bl != null) {
            return bl;
        }
        bl = new BlazingCacheManager(this, uri, classLoader, properties);
        managers.put(key, bl);
        return bl;
    }

    @Override
    public ClassLoader getDefaultClassLoader() {
        return this.getClass().getClassLoader();
    }

    @Override
    public URI getDefaultURI() {
        try {
            return new URI("blazingcache://vm");
        } catch (URISyntaxException err) {
            throw new RuntimeException(err);
        }
    }

    @Override
    public Properties getDefaultProperties() {
        Properties res = new Properties();
        res.put("blazingcache.usefetch", "true");
        res.put("blazingcache.mode", "local");
        return res;
    }

    private final Map<String, BlazingCacheManager> managers = new ConcurrentHashMap<>();

    @Override
    public CacheManager getCacheManager(URI uri, ClassLoader classLoader) {
        return getCacheManager(uri, classLoader, getDefaultProperties());
    }

    @Override
    public CacheManager getCacheManager() {
        return getCacheManager(getDefaultURI(), getDefaultClassLoader());
    }

    @Override
    public void close() {

    }

    @Override
    public void close(ClassLoader classLoader) {

    }

    @Override
    public void close(URI uri, ClassLoader classLoader) {
    }

    @Override
    public boolean isSupported(OptionalFeature optionalFeature) {
        return false;
    }

}
