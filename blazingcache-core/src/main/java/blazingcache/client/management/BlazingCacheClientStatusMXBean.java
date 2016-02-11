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
 * BlazingCache client status on JMX.
 *
 * @author matteo.casadei
 * @since 1.5.0
 *
 */
public class BlazingCacheClientStatusMXBean implements CacheClientStatusMXBean {

    /**
     * the cache client the provided status refers to.
     */
    private final CacheClient client;

    /**
     * Constructs a new MXBean to publish client status on JMX.
     *
     * @param client
     *            the {@see CacheClient} instance the status published by this
     *            MXBean refers to
     */
    public BlazingCacheClientStatusMXBean(final CacheClient client) {
        this.client = client;
    }

    /**
     * {@inheritDoc}
     */
    public long getCurrentTimestamp() {
        return this.client.getCurrentTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    public long getLastConnectionTimestamp() {
        return this.client.getConnectionTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isClientConnected() {
        return this.client.isConnected();
    }

    /**
     * {@inheritDoc}
     */
    public long getCacheConfiguredMaxMemory() {
        return this.client.getMaxMemory();
    }

    /**
     * {@inheritDoc}
     */
    public long getCacheUsedMemory() {
        return this.client.getActualMemory();
    }

    /**
     * {@inheritDoc}
     */
    public int getCacheSize() {
        return this.client.getCacheSize();
    }

    /**
     * {@inheritDoc}
     */
    public long getCacheOldestEvictedKeyAge() {
        return this.client.getOldestEvictedKeyAge();
    }

    /**
     * {@inheritDoc}
     */
    public String getClientId() {
        return this.client.getClientId();
    }

}
