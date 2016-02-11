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

import blazingcache.client.CacheClient;

/**
 * BlazingCache client statistics on JMX.
 *
 * @author matteo.casadei
 * @since 1.5.0
 *
 */
public class BlazingCacheClientStatisticsMXBean implements CacheClientStatisticsMXBean {

    /**
     * the cache client the provided statistics refers to.
     */
    private final CacheClient client;

    /**
     * Construct a new MXBean to publish client statistics on JMX.
     *
     * @param client
     *            the {@see CacheClient} instance on which statistics will be
     *            retrieved
     */
    public BlazingCacheClientStatisticsMXBean(final CacheClient client) {
        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        this.client.clearStatistics();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientPuts() {
        return this.client.getClientPuts();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientTouches() {
        return this.client.getClientTouches();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientGets() {
        return this.client.getClientGets();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientFetches() {
        return this.client.getClientFetches();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientEvictions() {
        return this.client.getClientEvictions();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientInvalidations() {
        return this.client.getClientInvalidations();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientHits() {
        return this.client.getClientHits();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientMissedGetsToSuccessfulFetches() {
        return this.client.getClientMissedGetsToSuccessfulFetches();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientMissedGetsToMissedFetches() {
        return this.client.getClientMissedGetsToMissedFetches();
    }

    /**
     * {@inheritDoc}
     */
    public long getClientConflicts() {
        throw new UnsupportedOperationException("Value not supported yet!");
    }

}
