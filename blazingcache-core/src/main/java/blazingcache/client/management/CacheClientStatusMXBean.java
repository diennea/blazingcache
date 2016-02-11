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
 * Each client cache's status object is registered with an unique ObjectName with
 * the following type:
 * <p>
 * Type:
 * <code>blazingcache.client.management:type=CacheClientStatus</code>
 * <p>
 * Required Attributes:
 * <ul>
 * <li>CacheClient the id of the client cache</li>
 * </ul>
 *
 * @author matteo.casadei
 * @since 1.5.0
 */
public interface CacheClientStatusMXBean {

    /**
     * The id of the client.
     *
     * @return the id of the client
     */
    String getClientId();

    /**
     * The current timestamp of the client.
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
     * The maximum cache size in bytes configured on the client.
     *
     * @return the cache's max memory size
     */
    long getCacheConfiguredMaxMemory();

    /**
     * The amount of memory currently used by client's local cache.
     *
     * @return the amount memory used by the local cache
     */
    long getCacheUsedMemory();

    /**
     * The number of keys currently stored in client's local cache.
     *
     * @return the numbers of keys in cache
     */
    int getCacheSize();

    /**
     * The age in ns of the oldest key last evicted from the local cache.
     * <p>
     * Simply put, the oldest key is calculated referring always to last eviction execution.
     * <p>
     * In case of no eviction executed on the cache yet, 0 will be returned.
     *
     * @return the age of the oldest evicted key.
     */
    long getCacheLastEvictionOldestKeyAge();

}
