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
package blazingcache.client;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Nothing else than a class implemeting utility methods for better testing.
 *
 * @author matteo.casadei
 *
 */
public final class CacheClientTestUtils {

    /**
     *
     */
    private CacheClientTestUtils() {
    }

    /**
     * Fill the local cache of the specified client with the number of entries
     * specified, adopting as a value the byte array specifed by dataPattern.
     * <p>
     * For each entry, the method takes care of generate a unique key, corresponding to the insertion nanotime.
     *
     * @param client
     *            the cache client
     * @param dataPattern
     *            the array of bytes used as the values for the entries to be
     *            put into the local cache
     * @param numberOfEntries
     *            the number of entries to put into the cache
     * @param expireTime
     *            expire time to be applyed to every inserted entry
     * @return the set of automatically generated keys
     */
    public static Set<String> fillCacheWithTestData(final CacheClient client, final byte[] dataPattern, final int numberOfEntries,
            final long expireTime) {
        final Set<String> insertedKeys = new HashSet<>();

        Stream.generate(() -> System.nanoTime())
        .limit(numberOfEntries)
        .forEach(key -> {
            try {
                client.put(key.toString(), dataPattern, expireTime);
                insertedKeys.add(key.toString());
            } catch (CacheException | InterruptedException e) {
                // FIXME
                throw new RuntimeException("Impossible to fill cache with specified data", e);
            }
        });

        return insertedKeys;
    }

}
