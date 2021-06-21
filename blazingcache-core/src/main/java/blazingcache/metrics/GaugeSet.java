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
 * Simple metric that can be added to or removed from.
 *
 * @author dennis.mercuriali
 */
public interface GaugeSet {

    /**
     * Increment this metric value.
     * 
     * @param entryKey key of the entry which triggered this event. Might be {@code null}
     */
    void inc(RawString entryKey);

    /**
     * Decrement this metric value.
     * 
     * @param entryKey key of the entry which triggered this event. Might be {@code null}
     */
    void dec(RawString entryKey);

    /**
     * Add <code>value</code> to this metric value.
     *
     * @param value
     * @param entryKey key of the entry which triggered this event. Might be {@code null}
     */
    void add(long value, RawString entryKey);

    /**
     * Clear this metric value.
     */
    void clear();

    /**
     * Get this metric current value.
     * 
     * @return current value.
     */
    Long get();
}
