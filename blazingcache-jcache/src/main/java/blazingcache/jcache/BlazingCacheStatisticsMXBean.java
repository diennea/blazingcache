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
package blazingcache.jcache;

import java.io.Serializable;
import javax.cache.management.CacheStatisticsMXBean;

/**
 * Statistics on JMX
 *
 * @author enrico.olivelli
 */
public class BlazingCacheStatisticsMXBean<K, V> implements CacheStatisticsMXBean, Serializable {

    private static final long serialVersionUID = 1200L;
    private final transient BlazingCacheCache<K, V> cache;

    public BlazingCacheStatisticsMXBean(BlazingCacheCache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void clear() {
        cache.clearStatistics();
    }

    @Override
    public long getCacheHits() {
        return cache.getCacheHits();
    }

    @Override
    public float getCacheHitPercentage() {
        float res = cache.getCacheHits() * 100f / (cache.getCacheGets());
        if (Float.isNaN(res)) {
            return 0;
        }
        return res;
    }

    @Override
    public long getCacheMisses() {
        return cache.getCacheMisses();
    }

    @Override
    public float getCacheMissPercentage() {
        float res = cache.getCacheMisses() * 100f / (cache.getCacheGets());
        if (Float.isNaN(res)) {
            return 0;
        }
        return res;
    }

    @Override
    public long getCacheGets() {
        return cache.getCacheGets();
    }

    @Override
    public long getCachePuts() {
        return cache.getCachePuts();
    }

    @Override
    public long getCacheRemovals() {
        return cache.getCacheRemovals();
    }

    @Override
    public long getCacheEvictions() {
        return cache.getCacheEvictions();
    }

    @Override
    public float getAverageGetTime() {
        return cache.getAverageGetTime();
    }

    @Override
    public float getAveragePutTime() {
        return cache.getAveragePutTime();
    }

    @Override
    public float getAverageRemoveTime() {
        return cache.getAverageRemoveTime();
    }

}
