/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package blazingcache.client;

import blazingcache.client.impl.JDKEntrySerializer;
import blazingcache.network.ServerHostData;
import blazingcache.network.ServerLocator;
import blazingcache.network.jvm.JVMServerLocator;
import blazingcache.network.netty.GenericNettyBrokerLocator;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import blazingcache.zookeeper.ZKCacheServerLocator;

/**
 * Utility for booting CacheClients
 *
 * @author enrico.olivelli
 */
public class CacheClientBuilder {

    private String clientId = "localhost_" + System.nanoTime();
    private String clientSecret = "blazingcache";
    private Mode mode = Mode.LOCAL;
    private long maxMemory = 0;
    private long maxLocalEntryAge = 0;
    private int connectTimeout = 10000;
    private int socketTimeout = 0;
    private int fetchPriority = 10;
    private ServerLocator locator;
    private String zkConnectString = "localhost:1281";
    private int zkSessionTimeout = 40000;
    private String zkPath = "/blazingcache";
    private String host = "localhost";
    private Object cacheServer;
    private int port = 1025;
    private boolean ssl = false;
    private boolean jmx = false;
    private EntrySerializer entrySerializer = new JDKEntrySerializer();

    public static enum Mode {
        SINGLESERVER,
        CLUSTERED,
        LOCAL
    }

    private CacheClientBuilder() {

    }

    public static CacheClientBuilder newBuilder() {
        return new CacheClientBuilder();
    }

    /**
     * The the ID of the client, it MUST be unique, it represent the peer on the network.
     *
     * @param clientId
     * @return
     */
    public CacheClientBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    /**
     * Assign a priority to be used when a client is to be choosen for serving a remote fetch. Setting fetchPriority to
     * 0 will prevent this client from being asked to serve fetch requests from other clients
     *
     * @param fetchPriority
     * @return
     */
    public CacheClientBuilder fetchPriority(int fetchPriority) {
        this.fetchPriority = fetchPriority;
        return this;
    }

    /**
     * Assign an EntrySerializer to the CacheClient
     *
     * @param entrySerializer
     * @return
     */
    public CacheClientBuilder entrySerializer(EntrySerializer entrySerializer) {
        this.entrySerializer = entrySerializer;
        return this;
    }

    /**
     * Zookeeper Path for discovery.
     *
     * @param zkPath
     * @return
     */
    public CacheClientBuilder zkPath(String zkPath) {
        this.zkPath = zkPath;
        return this;
    }

    /**
     * Zookeeper connection string.
     *
     * @param zkConnectString
     * @return
     */
    public CacheClientBuilder zkConnectString(String zkConnectString) {
        this.zkConnectString = zkConnectString;
        return this;
    }

    /**
     * Pass a custom local CacheServer for LOCAL mode.
     *
     * @param cacheServer
     * @return
     */
    public CacheClientBuilder localCacheServer(Object cacheServer) {
        this.cacheServer = cacheServer;
        return this;
    }

    /**
     * Timeout for the Zookeeper client.
     */
    public CacheClientBuilder zkSessionTimeout(int zkSessionTimeout) {
        this.zkSessionTimeout = zkSessionTimeout;
        return this;
    }

    /**
     * Limit on the memory retained by the cache, the value is expressed in bytes.
     *
     * @param maxMemory
     * @return
     */
    public CacheClientBuilder maxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
        return this;
    }

    /**
     * Maximum "local" age of any entry (in millis). Sometimes a client retains "immortal" entries which does not need
     * anymore and continues to receive notifications. This options evicts automatically every entry which is too
     * old.<br>
     * This option also ensures that you are not going to keep data which could be stale if the client which updated
     * real data (on database for instance) dies (halt/crash) before invalidating the cache
     */
    public CacheClientBuilder maxLocalEntryAge(long maxLocalEntryAge) {
        this.maxLocalEntryAge = maxLocalEntryAge;
        return this;
    }

    /**
     * Port of the server for the SINGLESERVER mode.
     *
     * @param port
     * @return
     */
    public CacheClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    /**
     * Host of the server for the SINGLESERVER mode.
     *
     * @param host
     * @return
     */
    public CacheClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    /**
     * SSL mode for the SINGLESERVER mode.
     *
     * @param ssl
     * @return
     */
    public CacheClientBuilder ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    /**
     * JMX flag to enable publishing of JMX status and statistics.
     *
     * @param jmx true in order to enable publication of status and statistics mbeans on JMX
     * @return the instance of {
     * @see CacheClientBuilder}
     */
    public CacheClientBuilder jmx(final boolean jmx) {
        this.jmx = jmx;
        return this;
    }

    /**
     * Connection timeout for sockets.
     *
     * @param connectTimeout
     * @return
     */
    public CacheClientBuilder connectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    /**
     * Socket timeout for sockets.
     *
     * @param socketTimeout
     * @return
     */
    public CacheClientBuilder socketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    /**
     * Discovery mode.
     *
     * @param mode
     * @return
     * @see Mode
     */
    public CacheClientBuilder mode(Mode mode) {
        this.mode = mode;
        return this;
    }

    /**
     * Secret for autentication to the CacheServer.
     *
     * @param clientSecret
     * @return
     */
    public CacheClientBuilder clientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

    /**
     * Builds up the client. in LOCAL mode eventually a local embedded CacheServer will be started too. The returned
     * Client MUST be started in order to work.
     *
     * @return the new instance of {
     * @see CacheClient}
     * @see CacheClient#start()
     * @see CacheClient#waitForConnection(int)
     */
    public CacheClient build() {
        switch (mode) {
            case SINGLESERVER:
                locator = new NettyCacheServerLocator(host, port, ssl);
                ((GenericNettyBrokerLocator) locator).setConnectTimeout(connectTimeout);
                ((GenericNettyBrokerLocator) locator).setSocketTimeout(socketTimeout);
                break;
            case CLUSTERED:
                locator = new ZKCacheServerLocator(zkConnectString, zkSessionTimeout, zkPath);
                ((GenericNettyBrokerLocator) locator).setConnectTimeout(connectTimeout);
                ((GenericNettyBrokerLocator) locator).setSocketTimeout(socketTimeout);
                break;
            case LOCAL:
                if (cacheServer == null) {
                    cacheServer = new CacheServer(clientSecret, ServerHostData.LOCAL());
                    CacheServer cs = (CacheServer) cacheServer;
                    try {
                        cs.start();
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                    locator = new JVMServerLocator(cs, true);
                } else {
                    CacheServer cs = (CacheServer) cacheServer;
                    locator = new JVMServerLocator(cs, false);
                }
                break;
            default:
                throw new IllegalArgumentException("invalid mode " + mode);
        }
        final CacheClient res = new CacheClient(clientId, clientSecret, locator);
        res.setMaxMemory(maxMemory);
        res.setMaxLocalEntryAge(maxLocalEntryAge);
        res.setEntrySerializer(entrySerializer);
        res.setFetchPriority(fetchPriority);
        if (this.jmx) {
            res.enableJmx(true);
        }
        return res;
    }
}
