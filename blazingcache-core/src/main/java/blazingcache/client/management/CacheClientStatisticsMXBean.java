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
 * BlazingCache client's statistics.
 * <p>
 * Statistics are counted starting from client's last reboot.
 * <p>
 * Each cache's statistics object is registered with an unique ObjectName with
 * the following type:
 * <p>
 * Type:
 * <code>blazingcache.client.management:type=BlazingCacheClientStatistics</code>
 * <p>
 *
 * @author matteo.casadei
 * @since 1.4.2
 */
public interface CacheClientStatisticsMXBean {

    /**
     * The number of puts performed by the client since last reboot.
     *
     * @return the no. of puts
     */
    long getClientPuts();

    /**
     * The number of touches performed by the client since last reboot.
     *
     * @return the no. of touches
     */
    long getClientTouches();

    /**
     * The number of gets performed since last reboot.
     *
     * @return the no. of gets
     */
    long getClientGets();

    /**
     * The number of fetches since last reboot.
     *
     * @return the no. of fetches
     */
    long getClientFetches();

    /**
     * The number of evictions performed by the client since last reboot.
     *
     * @return the no. of evictions
     */
    long getClientEvictions();

    /**
     * The number of invalidations performed by the client since last reboot.
     *
     * @return the no. of invalidations
     */
    long getClientInvalidations();

    /**
     * The number of hits occurred in the client since last reboot.
     *
     * @return the no. of hits
     */
    long getClientHits();

    /**
     * The number unsuccessful gets followed by a corresponding successful fetch.
     * <p>
     * The value is devised since client's last reboot.
     *
     * @return the no. of miss to fetches
     */
    long getClientMissToFetches();

    /**
     * The total number of misses calculated as the sum of missed gets and corresponding
     * missed fetches.
     * <p>
     * This value is devised considering client's last reboot.
     *
     * @return the no. of miss to misses
     */
    long getClientMissesToMisses();

    /**
     * The number of conflicts detected by the client since last reboot.
     *
     * @return the no. of conflicts detected
     */
    long getClientConflicts();

}
