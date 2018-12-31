/*
 * Copyright 2015 Diennea S.R.L..
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

import java.util.Properties;

/**
 * Serializer for values
 *
 * @author enrico.olivelli
 */
public interface Serializer<K, V, V2> {

    /**
     * Serialize the give value to the value thet is to be written to the
     * underlying cache. For keys you MUST return a java.lang.String, for values
     * you MUST return a byte[]
     *
     * @param value
     * @return
     */
    public V2 serialize(K value);

    /**
     * Deserialize the given value from the raw type to the type required by the
     * javax.cache.Cache. For keys you will receive a java.lang.String, for
     * values you will receive a byte[]
     *
     * @param cachedValue
     * @return
     */
    public K deserialize(V cachedValue);

    /**
     * Read the actual CacheManager configuration. This method is called before any call to other methods
     *
     * @param properties
     */
    public default void configure(Properties properties) {
    }
;

};
