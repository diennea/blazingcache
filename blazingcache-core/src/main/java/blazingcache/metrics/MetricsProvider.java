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

/**
 *
 * @author dennis.mercuriali
 */
public interface MetricsProvider {

    /**
     * @param name
     * @return a {@code Gauge} metric logger
     */
    Gauge getGauge(String name);

    /**
     *
     * @param name
     * @return a {@code MetricStat} metric logger
     */
    MetricStat getMetricStat(String name);

    /**
     * Provides a {@code MetricsProvider} with the added scope {@code name}
     *
     * @param name
     * @return scoped {@code MetricsProvider}
     */
    MetricsProvider scope(String name);

    /**
     * Provides a {@code MetricProvider} without the given scope {@code name}
     *
     * @param name
     * @return
     */
    MetricsProvider removeScope(String name);
}
