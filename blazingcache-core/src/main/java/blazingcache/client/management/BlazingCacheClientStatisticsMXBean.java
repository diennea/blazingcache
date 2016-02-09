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
 * @since 1.4.2
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
    public long getClientPuts() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientTouches() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientGets() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientFetches() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientEvictions() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientInvalidations() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientHits() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientMissedGetsToSuccessfulFetches() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientMissedGetsToMissedFetches() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public long getClientConflicts() {
        // TODO Auto-generated method stub
        return 0;
    }

}
