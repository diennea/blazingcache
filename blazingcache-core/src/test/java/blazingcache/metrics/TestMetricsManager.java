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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author dennis.mercuriali
 */
public class TestMetricsManager {

    private Map<String, TestMetricsProvider.TestGauge> gauges;

    public TestMetricsManager() {
        gauges = new HashMap<>();
    }

    public Map<String, TestMetricsProvider.TestGauge> getGauges() {
        return gauges;
    }

    public TestMetricsProvider getProvider() {
        return new TestMetricsProvider("");
    }

    public class TestMetricsProvider implements MetricsProvider {

        private String scope;

        public TestMetricsProvider(String scope) {
            this.scope = scope;
        }

        @Override
        public GaugeSet getGaugeSet(String name) {
            TestGauge g = new TestGauge();
            gauges.put(getMetricName(name), g);
            return g;
        }

        @Override
        public MetricsProvider scope(String name) {
            String fullname = scope + "." + name;
            return new TestMetricsProvider(fullname);
        }

        @Override
        public MetricsProvider removeScope(String name) {
            return this;
        }

        private String getMetricName(String name) {
            return scope != null && !scope.isEmpty()
                    ? scope + "." + name
                    : name;
        }

        public class TestGauge implements GaugeSet {

            private ConcurrentHashMap<String, Long> subGauges = new ConcurrentHashMap<>();

            @Override
            public void clear() {
                subGauges.clear();
            }

            @Override
            public void inc(RawString key) {
                String subKey = getSubGaugeKey(key);
                subGauges.compute(subKey, (k, v) -> {
                    return v == null
                            ? 1L
                            : v + 1;
                });
            }

            @Override
            public void dec(RawString key) {
                String subKey = getSubGaugeKey(key);
                subGauges.compute(subKey, (k, v) -> {
                    return v == null
                            ? -1L
                            : v - 1;
                });
            }

            @Override
            public void add(long delta, RawString key) {
                String subKey = getSubGaugeKey(key);
                subGauges.compute(subKey, (k, v) -> {
                    return v == null
                            ? delta
                            : v + delta;
                });
            }

            @Override
            public Long get() {
                return subGauges.values().stream().mapToLong(Long::longValue).sum();
            }
            
            public Long get(String key) {
                return subGauges.get(key);
            }

            private String getSubGaugeKey(RawString key) {
                return key.toString();
            }
        }
    }
}
