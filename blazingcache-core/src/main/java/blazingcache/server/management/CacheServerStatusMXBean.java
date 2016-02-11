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
package blazingcache.server.management;

/**
 * BlazingCache server's status information.
 * <p>
 * Each cache server status object is registered with an unique ObjectName with
 * the following type:
 * <p>
 * Type:
 * <code>blazingcache.server.management:type=CacheServerStatus</code>
 * <p>
 * Required Attributes:
 * <ul>
 * <li>CacheServer the id of the cache server</li>
 * </ul>
 *
 * @author matteo.casadei
 * @since 1.5.0
 */
public interface CacheServerStatusMXBean {

    /**
     * Current server timestamp.
     *
     * @return the current timestamp
     */
    long getCurrentTimestamp();

    /**
     * Retrieves the current server mode: true if the server is the leader, false if it is acting as a backup server.
     *
     * @return true if the server is leader, false otherwise
     */
    boolean isLeader();

    /**
     * Returns the timestamp corresponding to the last time the server switched from leader to backup or the other way round.
     *
     * @return the timestamp corresponding to last server's state change
     */
    long getStateChangeTimestamp();

    /**
     * Returns the number of keys stored in all the caches present in the system.
     *
     * @return the "global" number of keys stores in every cache
     */
    int getGlobalCacheSize();

    /**
     * The number of clients connected to the server.
     *
     * @return the number of connected clients
     */
    int getConnectedClients();

    /**
     * The number of entries on which clients acquired (and still hold) a lock.
     *
     * @return the number of locked entries
     */
    int getLockedEntries();

    /**
     * Number of operations requested to the server still to be completed.
     *
     * @return the number of pending operations
     */
    long getPendingOperations();

}
