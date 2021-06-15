/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package blazingcache.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author dennis.mercuriali
 */
public class MonitoredAtomicLong {

    private final AtomicLong inner;
    private final Gauge gauge;

    public MonitoredAtomicLong(long initialValue, Gauge gauge) {
        this.inner = new AtomicLong(initialValue);
        this.gauge = gauge;
    }

    public final long get() {
        return inner.get();
    }

    public final long incrementAndGet() {
        gauge.inc();
        return inner.incrementAndGet();
    }

    public final long decrementAndGet() {
        gauge.dec();
        return inner.decrementAndGet();
    }

    public final long addAndGet(long delta) {
        gauge.add(delta);
        return inner.addAndGet(delta);
    }

    public final void reset() {
        gauge.clear();
        inner.set(0L);
    }
    
    public long longValue() {
        return inner.longValue();
    }

}
