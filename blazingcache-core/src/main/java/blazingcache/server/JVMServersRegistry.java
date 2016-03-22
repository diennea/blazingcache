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
package blazingcache.server;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JVM classloader based registry for Brokers
 *
 * @author enrico.olivelli
 */
public class JVMServersRegistry {

    private static final ConcurrentHashMap<String, CacheServer> servers = new ConcurrentHashMap<>();
    private static final Logger LOGGER = Logger.getLogger(JVMServersRegistry.class.getName());
    private static String lastRegisteredServer = "";

    public static void registerServer(String id, CacheServer broker) {
        LOGGER.log(Level.SEVERE, "registerServer {0}", id);
        servers.put(id, broker);
        lastRegisteredServer = id;
    }

    public static <T extends CacheServer> T lookupServer(String id) {
        return (T) servers.get(id);
    }

    public static <T extends CacheServer> T getDefaultServer() {
        return (T) servers.get(lastRegisteredServer);
    }

    public static void clear() {
        servers.clear();
    }

    public static void unregisterServer(String brokerId) {
        servers.remove(brokerId);
    }
}
