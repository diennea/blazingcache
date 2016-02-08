/*
 * Copyright 2016 Diennea S.R.L..
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
package blazingcache.client.management;

/**
 * BlazingCache client's status information.
 * <p>
 * Each cache's status object is registered with an unique ObjectName with
 * the following type:
 * <p>
 * Type:
 * <code>blazingcache.client.management:type=BlazingCacheClientStatus</code>
 * <p>
 *
 * @author matteo.casadei
 * @since 1.4.2
 */
public interface CacheClientStatusMXBean {

    /**
     * The current timestamp of the client in milliseconds.
     *
     * @return the current timestamp
     */
    long getCurrentTimestamp();

    /**
     * The timestamp of the last time the client connected to the server.
     *
     * @return the last connection timestamp
     */
    long getLastConnectionTimestamp();

    /**
     * The status of client connection to the server.
     *
     * @return true if the client is currently connected to the server
     */
    boolean isClientConnected();

    /**
     * The maximum size of the cache memory configured on the client.
     *
     * @return the cache's max memory size
     */
    long getCacheConfiguredMaxMemory();

    /**
     * The amount of memory used by the client's cache.
     *
     * @return the amount of cache used memory
     */
    long getCacheUsedMemory();

    /**
     * The number of keys currently stored in client's cache.
     *
     * @return the numbers of keys in cache
     */
    long getCacheNumberOfKeys();

    /**
     * The timestamp corresponding to the oldest key currently stored in client's cache.
     *
     * @return the timestamp of the oldest key
     */
    long getCacheOldestKeyTimestamp();

}
