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

import blazingcache.utils.RawString;

/**
 * No-op metrics provider.
 *
 * @author dennis.mercuriali
 */
public class NullMetricsProvider implements MetricsProvider {

    public static final NullMetricsProvider INSTANCE = new NullMetricsProvider();

    /**
     * A <i>no-op</i> {@code Counter}.
     */
    static class NullGaugeImpl implements Gauge {

        @Override
        public void inc(RawString entryKey) {
            // noop
        }

        @Override
        public void dec(RawString entryKey) {
            // noop
        }

        @Override
        public void add(long value, RawString entryKey) {
            // noop
        }

        @Override
        public void clear() {
            // noop
        }

        @Override
        public Long get() {
            return 0L;
        }
    }

    static Gauge nullGauge = new NullGaugeImpl();

    @Override
    public Gauge getGauge(String name) {
        return nullGauge;
    }

    @Override
    public MetricsProvider scope(String name) {
        return this;
    }

    @Override
    public MetricsProvider removeScope(String name) {
        return this;
    }
}
