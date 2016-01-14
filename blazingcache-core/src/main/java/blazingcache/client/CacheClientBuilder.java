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

    private String clientId;
    private String clientSecret = "blazingcache";
    private Mode mode = Mode.SINGLESERVER;
    private long maxMemory = 0;
    private int connectTimeout = 10000;
    private int socketTimeout = 0;
    private ServerLocator locator;
    private String zkConnectString = "localhost:1281";
    private int zkSessionTimeout = 40000;
    private String zkPath = "/blazingcache";
    private String host = "localhost";
    private Object cacheServer;
    private int port = 1025;
    private boolean ssl = false;

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

    public CacheClientBuilder clientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public CacheClientBuilder zkPath(String zkPath) {
        this.zkPath = zkPath;
        return this;
    }

    public CacheClientBuilder zkConnectString(String zkConnectString) {
        this.zkConnectString = zkConnectString;
        return this;
    }

    public CacheClientBuilder localCacheServer(Object cacheServer) {
        this.cacheServer = cacheServer;
        return this;
    }

    public CacheClientBuilder zkSessionTimeout(int zkSessionTimeout) {
        this.zkSessionTimeout = zkSessionTimeout;
        return this;
    }

    public CacheClientBuilder maxMemory(long maxMemory) {
        this.maxMemory = maxMemory;
        return this;
    }

    public CacheClientBuilder port(int port) {
        this.port = port;
        return this;
    }

    public CacheClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    public CacheClientBuilder ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }

    public CacheClientBuilder connectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public CacheClientBuilder socketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public CacheClientBuilder mode(Mode mode) {
        this.mode = mode;
        return this;
    }

    public CacheClientBuilder clientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
        return this;
    }

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
                CacheServer cs = (CacheServer) cacheServer;
                locator = new JVMServerLocator(cs);                
                break;
            default:
                throw new IllegalArgumentException("invalid mode " + mode);
        }
        CacheClient res = new CacheClient(clientId, clientSecret, locator);
        res.setMaxMemory(maxMemory);
        return res;
    }
}
