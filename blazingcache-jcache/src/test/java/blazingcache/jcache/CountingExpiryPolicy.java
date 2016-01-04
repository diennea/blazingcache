/*
 * Copyright 2016 enrico.olivelli.
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
package blazingcache.jcache;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

/**
 * Taken fomr the TcK
 *
 * @author enrico.olivelli
 */
public class CountingExpiryPolicy implements ExpiryPolicy, Serializable {

    /**
     * The number of times {@link #getExpiryForCreation()} was called.
     */
    private AtomicInteger creationCount;

    /**
     * The number of times {@link #getExpiryForAccess()} ()} was called.
     */
    private AtomicInteger accessedCount;

    /**
     * The number of times {@link #getExpiryForUpdate()} was called.
     */
    private AtomicInteger updatedCount;

    /**
     * Constructs a new {@link CountingExpiryPolicy}.
     */
    public CountingExpiryPolicy() {
        this.creationCount = new AtomicInteger(0);
        this.accessedCount = new AtomicInteger(0);
        this.updatedCount = new AtomicInteger(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Duration getExpiryForCreation() {
        new Exception().printStackTrace();
        creationCount.incrementAndGet();
        return Duration.ETERNAL;
    }

    /**
     * Obtains the number of times {@link #getExpiryForCreation()} has been
     * called.
     *
     * @return the count
     */
    public int getCreationCount() {
        return creationCount.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Duration getExpiryForAccess() {
        new Exception().printStackTrace();
        accessedCount.incrementAndGet();
        return null;
    }

    /**
     * Obtains the number of times {@link #getExpiryForAccess()} has been
     * called.
     *
     * @return the count
     */
    public int getAccessCount() {
        return accessedCount.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Duration getExpiryForUpdate() {
        new Exception().printStackTrace();
        updatedCount.incrementAndGet();
        return null;
    }

    /**
     * Obtains the number of times {@link #getExpiryForUpdate()} has been
     * called.
     *
     * @return the count
     */
    public int getUpdatedCount() {
        return updatedCount.get();
    }

    public void resetCount() {
        creationCount.set(0);
        accessedCount.set(0);
        updatedCount.set(0);
    }
}
