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
import blazingcache.client.EntrySerializer;
import blazingcache.network.ServerHostData;
import blazingcache.network.ServerLocator;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
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

    final static boolean JSR107_TCK_101_COMPAT_MODE = Boolean.getBoolean("org.blazingcache.jsr107tck101compatmode");

    private final BlazingCacheProvider provider;
    private final URI uri;
    private final ClassLoader classLoader;
    private final Properties properties;
    private volatile boolean closed;
    private final CacheClient client;
    private final CacheServer embeddedServer;
    private final boolean usefetch;
    private final int fetchPriority;
    private final Serializer<Object, String> keysSerializer;
    private final Serializer<Object, byte[]> valuesSerializer;
    private final Map<String, BlazingCacheCache> caches = new HashMap<>();

    BlazingCacheManager(BlazingCacheProvider provider, URI uri, ClassLoader classLoader, Properties configproperties) {
        try {
            this.provider = provider;
            this.uri = uri;

            Properties properties_and_params = new Properties();
            parseUriQueryStringParameters(uri, properties_and_params);
            properties_and_params.putAll(configproperties);
            String configfile = properties_and_params.getProperty("configfile", "blazingcache.properties");
            if (!configfile.isEmpty()) {
                loadConfigFile(configfile, "configure", classLoader, properties_and_params);
                try {
                    loadConfigFile(configfile, "context", Thread.currentThread().getContextClassLoader(), properties_and_params);
                } catch (SecurityException cannotAccess) {
                    LOG.log(Level.CONFIG, "Cannot access context classloader:" + cannotAccess);
                }
            }
            this.usefetch = Boolean.parseBoolean(properties_and_params.getProperty("blazingcache.usefetch", "true"));
            this.fetchPriority = Integer.parseInt(properties_and_params.getProperty("blazingcache.fetchpriority", "10"));
            this.classLoader = classLoader;
            this.properties = properties_and_params;
            String keySerializerClass = properties_and_params.getProperty("blazingcache.jcache.keyserializer", "blazingcache.jcache.StandardKeySerializer");
            String valuesSerializerClass = properties_and_params.getProperty("blazingcache.jcache.valuesserializer", "blazingcache.jcache.StandardValuesSerializer");
            long maxmemory = Long.parseLong(properties_and_params.getProperty("blazingcache.jcache.maxmemory", "0"));
            long maxLocalEntryAge = Long.parseLong(properties_and_params.getProperty("blazingcache.jcache.localentryage", "0"));
            this.keysSerializer = (Serializer<Object, String>) Class.forName(keySerializerClass, true, classLoader).newInstance();
            this.valuesSerializer = (Serializer<Object, byte[]>) Class.forName(valuesSerializerClass, true, classLoader).newInstance();
            this.keysSerializer.configure(properties_and_params);
            this.valuesSerializer.configure(properties_and_params);
            String clientId = properties_and_params.getProperty("blazingcache.clientId", "client");
            String secret = properties_and_params.getProperty("blazingcache.secret", "blazingcache");
            final boolean jmx = Boolean.parseBoolean(properties_and_params.getProperty("blazingcache.jmx", "false"));
            if (clientId.isEmpty()) {
                try {
                    clientId = InetAddress.getLocalHost().getCanonicalHostName();
                } catch (UnknownHostException err) {
                    throw new RuntimeException(err);
                }
            }
            ServerLocator locator;
            String mode = properties_and_params.getProperty("blazingcache.mode", "local");
            int sockettimeout = Integer.parseInt(properties_and_params.getProperty("blazingcache.zookeeper.sockettimeout", "0"));
            int connecttimeout = Integer.parseInt(properties_and_params.getProperty("blazingcache.zookeeper.connecttimeout", "10000"));
            switch (mode) {
                case "clustered": {
                    String connect = properties_and_params.getProperty("blazingcache.zookeeper.connectstring", "localhost:1281");
                    int timeout = Integer.parseInt(properties_and_params.getProperty("blazingcache.zookeeper.sessiontimeout", "40000"));
                    String path = properties_and_params.getProperty("blazingcache.zookeeper.path", "/blazingcache");
                    locator = new ZKCacheServerLocator(connect, timeout, path);
                    ((ZKCacheServerLocator) locator).setSocketTimeout(sockettimeout);
                    ((ZKCacheServerLocator) locator).setConnectTimeout(connecttimeout);
                    this.client = new CacheClient(clientId, secret, locator);
                    this.embeddedServer = null;
                }
                break;
                case "server": {
                    String connect = properties_and_params.getProperty("blazingcache.zookeeper.connectstring", "localhost:1281");
                    int timeout = Integer.parseInt(properties_and_params.getProperty("blazingcache.zookeeper.sessiontimeout", "40000"));
                    String path = properties_and_params.getProperty("blazingcache.zookeeper.path", "/blazingcache");
                    boolean writeacls = Boolean.parseBoolean(properties_and_params.getProperty("blazingcache.zookeeper.writeacls", "false"));
                    String host = properties_and_params.getProperty("blazingcache.server.host", "");
                    int port = Integer.parseInt(properties_and_params.getProperty("blazingcache.server.port", "7000"));
                    boolean ssl = Boolean.parseBoolean(properties_and_params.getProperty("blazingcache.server.ssl", "false"));
                    if (host.isEmpty()) {
                        host = InetAddress.getLocalHost().getCanonicalHostName();
                    }
                    locator = new ZKCacheServerLocator(connect, timeout, path);
                    ((ZKCacheServerLocator) locator).setSocketTimeout(sockettimeout);
                    ((ZKCacheServerLocator) locator).setConnectTimeout(connecttimeout);
                    this.client = new CacheClient(clientId, secret, locator);
                    ServerHostData hostData = new ServerHostData(host, port, "", ssl, new HashMap<>());
                    this.embeddedServer = new CacheServer(secret, hostData);
                    this.embeddedServer.setupCluster(connect, timeout, path, hostData, writeacls);
                    break;
                }
                case "static": {
                    String host = properties_and_params.getProperty("blazingcache.server.host", "localhost");
                    int port = Integer.parseInt(properties_and_params.getProperty("blazingcache.server.port", "1025"));
                    boolean ssl = Boolean.parseBoolean(properties_and_params.getProperty("blazingcache.server.ssl", "false"));
                    locator = new NettyCacheServerLocator(host, port, ssl);
                    ((NettyCacheServerLocator) locator).setSocketTimeout(sockettimeout);
                    ((NettyCacheServerLocator) locator).setConnectTimeout(connecttimeout);
                    this.client = new CacheClient(clientId, secret, locator);
                    this.embeddedServer = null;
                    break;
                }
                case "local": {
                    this.embeddedServer = new CacheServer(secret, new ServerHostData("localhost", -1, "", false, new HashMap<>()));
                    if (JSR107_TCK_101_COMPAT_MODE) {
                        this.embeddedServer.setExpirerPeriod(1);
                    }
                    locator = new blazingcache.network.jvm.JVMServerLocator(embeddedServer, false);
                    this.client = new CacheClient(clientId, secret, locator);
                    break;
                }
                default:
                    throw new RuntimeException("unsupported blazingcache.mode=" + mode);
            }
            if (embeddedServer != null) {
                embeddedServer.start();
            }
            if (jmx) {
                client.enableJmx(true);
            }
            client.setEntrySerializer(new EntrySerializer() {
                @Override
                public byte[] serializeObject(String key, Object object) throws blazingcache.client.CacheException {
                    return valuesSerializer.serialize(object);
                }

                @Override
                public Object deserializeObject(String key, byte[] value) throws blazingcache.client.CacheException {
                    return valuesSerializer.deserialize(value);
                }
            });
            client.setMaxMemory(maxmemory);
            client.setMaxLocalEntryAge(maxLocalEntryAge);
            client.setFetchPriority(fetchPriority);
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
    public <K, V, C extends Configuration<K, V>>
        Cache<K, V> createCache(String cacheName, C configuration) throws IllegalArgumentException {
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
    @SuppressWarnings("unchecked")
    public <K, V> Cache<K, V> getCache(String cacheName) {
        checkClosed();
        if (cacheName == null) {
            throw new NullPointerException();
        }
        return caches.get(cacheName);
    }

    @Override
    public Iterable<String> getCacheNames() {
        if (!JSR107_TCK_101_COMPAT_MODE) {
            checkClosed();
        }
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

    private void parseUriQueryStringParameters(URI uri, Properties properties_and_params) {
        if (uri == null) {
            return;
        }
        try {
            String url = uri.toString();
            int questionMark = url.indexOf('?');
            if (questionMark < url.length()) {
                String qs = url.substring(questionMark + 1);
                String[] params = qs.split("&");
                for (String param : params) {
                    int pos = param.indexOf('=');
                    if (pos > 0) {
                        String key = param.substring(0, pos);
                        if (!key.startsWith("blazingcache.")) {
                            key = "blazingcache." + key;
                        }
                        String value = URLDecoder.decode(param.substring(pos + 1), "utf-8");
                        properties_and_params.put(key, value);
                    }
                }
            }
        } catch (Exception err) {
            LOG.log(Level.SEVERE, "Error parsing URI: " + uri, err);
        }
    }
    private static final Logger LOG = Logger.getLogger(BlazingCacheManager.class.getName());

    private void loadConfigFile(String configfile, String classLoaderName, ClassLoader classLoader, Properties properties_and_params) {

        InputStream in = classLoader.getResourceAsStream(configfile);
        if (in != null) {
            LOG.log(Level.CONFIG, "Loading configuration from resource " + configfile + " from " + classLoaderName + " classloader");
            try (InputStream read = in) {
                Properties p = new Properties();
                p.load(read);
                properties_and_params.putAll(p);
            } catch (IOException err) {
                LOG.log(Level.SEVERE, "Error while configuration from resource " + configfile + " from " + classLoaderName + " classloader: " + err, err);
            }
        } else {
            LOG.log(Level.CONFIG, "Cannot find resource " + configfile + " from " + classLoaderName + " classloader");
        }
    }

}
