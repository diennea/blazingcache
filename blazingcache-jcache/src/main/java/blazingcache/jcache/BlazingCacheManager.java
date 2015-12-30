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

import blazingcache.client.CacheClient;
import blazingcache.client.CacheEntry;
import blazingcache.client.events.CacheClientEventListener;
import blazingcache.network.ServerHostData;
import blazingcache.network.ServerLocator;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;

/**
 * Implementation of JSR 107 CacheManager
 *
 * @author enrico.olivelli
 */
public class BlazingCacheManager implements CacheManager {

    private final BlazingCacheProvider provider;
    private final URI uri;
    private final ClassLoader classLoader;
    private final Properties properties;
    private volatile boolean closed;
    private final CacheClient client;
    private final boolean usefetch;
    private final Serializer<Object, String> keysSerializer;
    private final Serializer<Object, byte[]> valuesSerializer;
    private final Map<String, BlazingCacheCache> caches = new HashMap<>();

    BlazingCacheManager(BlazingCacheProvider provider, URI uri, ClassLoader classLoader, Properties properties) {
        try {
            this.provider = provider;
            this.uri = uri;
            this.usefetch = Boolean.parseBoolean(properties.getProperty("blazingcache.usefetch", "true"));
            this.classLoader = classLoader;
            this.properties = properties;
            String keySerializerClass = properties.getProperty("blazingcache.jcache.keyserializer", "blazingcache.jcache.StandardKeySerializer");
            String valuesSerializerClass = properties.getProperty("blazingcache.jcache.keyserializer", "blazingcache.jcache.StandardValuesSerializer");
            this.keysSerializer = (Serializer<Object, String>) Class.forName(keySerializerClass, true, classLoader).newInstance();
            this.valuesSerializer = (Serializer<Object, byte[]>) Class.forName(valuesSerializerClass, true, classLoader).newInstance();
            String clientId = properties.getProperty("blazingcache.clientId", "client");
            String secret = properties.getProperty("blazingcache.secret", "blazingcache");
            if (clientId.isEmpty()) {
                try {
                    clientId = InetAddress.getLocalHost().getCanonicalHostName();
                } catch (UnknownHostException err) {
                    throw new RuntimeException(err);
                }
            }
            ServerLocator locator;
            String mode = properties.getProperty("blazingcache.mode", "local");
            switch (mode) {
                case "zk":
                    String connect = properties.getProperty("blazingcache.zookeeper.connectstring", "localhost");
                    int timeout = Integer.parseInt(properties.getProperty("blazingcache.zookeeper.sessiontimeout", "40000"));
                    String path = properties.getProperty("blazingcache.zookeeper.path", "/cache");
                    locator = new ZKCacheServerLocator(connect, timeout, path);
                    this.client = new CacheClient(clientId, secret, locator);
                    break;
                case "static":
                    String host = properties.getProperty("blazingcache.server.host", "localhost");
                    int port = Integer.parseInt(properties.getProperty("blazingcache.server.port", "1025"));
                    boolean ssl = Boolean.parseBoolean(properties.getProperty("blazingcache.server.ssl", "false"));
                    locator = new NettyCacheServerLocator(host, port, ssl);
                    this.client = new CacheClient(clientId, secret, locator);
                    break;
                case "local":
                    locator = new blazingcache.network.mock.MockServerLocator();
                    this.client = new CacheClient(clientId, secret, locator);
                    break;
                default:
                    throw new RuntimeException("unsupported blazingcache.mode=" + mode);
            }
            client.start();
            boolean ok = client.waitForConnection(10000);
            if (!ok) {
                System.out.println("Connection could not established");
            } else {
                System.out.println("Connection OK");
            }
        } catch (Exception err) {
            throw new CacheException(err);
        }
    }

    private BlazingCacheCache getCacheByKey(String key) {
        String cacheName = BlazingCacheCache.getCacheName(key);
        if (cacheName != null) {
            return caches.get(cacheName);
        }
        return null;
    }

    @Override
    public CachingProvider getCachingProvider() {
        return provider;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public <K, V, C extends Configuration<K, V>> Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
        BlazingCacheCache c = new BlazingCacheCache(cacheName, client, this, keysSerializer, valuesSerializer, usefetch, configuration);
        return c;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        return caches.get(cacheName);
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        return caches.get(cacheName);
    }

    @Override
    public Iterable<String> getCacheNames() {
        return caches.keySet();
    }

    @Override
    public void destroyCache(String cacheName) {
        BlazingCacheCache removed = caches.remove(cacheName);
        if (removed != null) {
            removed.clear();
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        // noop
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        // noop
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception err) {
            }
        }
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        return null;
    }

}
