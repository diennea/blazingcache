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

import blazingcache.client.CacheClient;
import static blazingcache.jcache.BlazingCacheCache.JSR107_TCK_101_COMPAT_MODE;
import blazingcache.network.ServerHostData;
import blazingcache.network.ServerLocator;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
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
    private final CacheServer embeddedServer;
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
            long maxmemory = Long.parseLong(properties.getProperty("blazingcache.jcache.maxmemory", "0"));
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
            int sockettimeout = Integer.parseInt(properties.getProperty("blazingcache.zookeeper.sockettimeout", "0"));
            int connecttimeout = Integer.parseInt(properties.getProperty("blazingcache.zookeeper.connecttimeout", "10000"));
            switch (mode) {
                case "zk":
                    String connect = properties.getProperty("blazingcache.zookeeper.connectstring", "localhost");
                    int timeout = Integer.parseInt(properties.getProperty("blazingcache.zookeeper.sessiontimeout", "40000"));
                    String path = properties.getProperty("blazingcache.zookeeper.path", "/cache");
                    locator = new ZKCacheServerLocator(connect, timeout, path);
                    ((ZKCacheServerLocator) locator).setSocketTimeout(sockettimeout);
                    ((ZKCacheServerLocator) locator).setConnectTimeout(connecttimeout);
                    this.client = new CacheClient(clientId, secret, locator);
                    this.embeddedServer = null;
                    break;
                case "static":
                    String host = properties.getProperty("blazingcache.server.host", "localhost");
                    int port = Integer.parseInt(properties.getProperty("blazingcache.server.port", "1025"));
                    boolean ssl = Boolean.parseBoolean(properties.getProperty("blazingcache.server.ssl", "false"));
                    locator = new NettyCacheServerLocator(host, port, ssl);
                    ((NettyCacheServerLocator) locator).setSocketTimeout(sockettimeout);
                    ((NettyCacheServerLocator) locator).setConnectTimeout(connecttimeout);
                    this.client = new CacheClient(clientId, secret, locator);
                    this.embeddedServer = null;
                    break;
                case "local":
                    this.embeddedServer = new CacheServer(secret, new ServerHostData("localhost", -1, "", false, new HashMap<>()));
                    if (JSR107_TCK_101_COMPAT_MODE) {
                        this.embeddedServer.setExpirerPeriod(1);
                    }
                    locator = new blazingcache.network.jvm.JVMBrokerLocator("localhost", embeddedServer);
                    this.client = new CacheClient(clientId, secret, locator);
                    break;
                default:
                    throw new RuntimeException("unsupported blazingcache.mode=" + mode);
            }
            if (embeddedServer != null) {
                embeddedServer.start();
            }
            client.setMaxMemory(maxmemory);
            client.start();
            client.waitForConnection(10000);

        } catch (Exception err) {
            throw new CacheException(err);
        }
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
        checkClosed();
        if (cacheName == null || configuration == null) {
            throw new NullPointerException();
        }
        BlazingCacheCache c = new BlazingCacheCache(cacheName, client, this, keysSerializer, valuesSerializer, usefetch, configuration);
        if (caches.putIfAbsent(cacheName, c) != null) {
            throw new CacheException("A cache with name " + cacheName + " already exists");
        }
        return c;
    }

    private void checkClosed() {
        if (closed) {
            throw new IllegalStateException("this CacheManager is closed");
        }
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName, Class<K> keyType, Class<V> valueType) {
        checkClosed();
        if (keyType == null || valueType == null) {
            throw new NullPointerException();
        }
        if (cacheName == null) {
            throw new NullPointerException();
        }
        Cache<K, V> res = caches.get(cacheName);
        if (res == null) {
            return null;
        }
        Configuration configuration = res.getConfiguration(Configuration.class);
        if ((!keyType.equals(configuration.getKeyType()))
                || !valueType.equals(configuration.getValueType())) {
            throw new ClassCastException();
        }
        return res;
    }

    @Override
    public <K, V> Cache<K, V> getCache(String cacheName) {
        checkClosed();
        if (cacheName == null) {
            throw new NullPointerException();
        }
        Cache<K, V> res = caches.get(cacheName);
        if (res == null) {
            return null;
        }
        Configuration configuration = res.getConfiguration(Configuration.class);
        if ((configuration.getKeyType() != null && !configuration.getKeyType().equals(Object.class))
                || (configuration.getValueType() != null && !configuration.getValueType().equals(Object.class))) {
            throw new IllegalArgumentException();
        }
        return res;
    }

    @Override
    public Iterable<String> getCacheNames() {
        //checkClosed(); TCK does not pass if we throw IllegalStateException
        return Collections.unmodifiableCollection(new ArrayList<>(caches.keySet()));
    }

    @Override
    public void destroyCache(String cacheName) {
        checkClosed();
        if (cacheName == null) {
            throw new NullPointerException();
        }
        BlazingCacheCache removed = caches.remove(cacheName);
        if (removed != null) {
            removed.close();
        }
    }

    @Override
    public void enableManagement(String cacheName, boolean enabled) {
        checkClosed();
        if (cacheName == null) {
            throw new NullPointerException();
        }
        BlazingCacheCache cache = caches.get(cacheName);
        if (cache == null) {
            return;
        }
        cache.setManagementEnabled(enabled);
    }

    @Override
    public void enableStatistics(String cacheName, boolean enabled) {
        checkClosed();
        if (cacheName == null) {
            throw new NullPointerException();
        }
        BlazingCacheCache cache = caches.get(cacheName);
        if (cache == null) {
            return;
        }
        cache.setStatisticsEnabled(enabled);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        for (BlazingCacheCache cache : caches.values()) {
            cache.close();
        }
        caches.clear();
        if (client != null) {
            try {
                client.close();
            } catch (Exception err) {
            }
        }
        if (embeddedServer != null) {
            embeddedServer.close();
        }
        closed = true;
        provider.releaseCacheManager(uri, classLoader);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        } else {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Get the low-level client
     *
     * @return
     */
    public CacheClient getClient() {
        return client;
    }

}
