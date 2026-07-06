/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package blazingcache.server;

import static org.junit.Assert.assertNull;
import blazingcache.utils.RawString;
import org.junit.Test;

/**
 * A malformed (non-numeric) client-provided lock id must be reported as an invalid
 * lock (null) instead of throwing a NumberFormatException that would leave the
 * request hanging until timeout (issue #188).
 *
 * @author diego.salvi
 */
public class KeyedLockManagerLockIdTest {

    private static final RawString KEY = RawString.of("k");

    @Test
    public void malformedClientProvidedLockIdIsInvalidOnWrite() {
        KeyedLockManager m = new KeyedLockManager();
        assertNull(m.acquireWriteLockForKey(KEY, "client", "not-a-number"));
    }

    @Test
    public void malformedClientProvidedLockIdIsInvalidOnRead() {
        KeyedLockManager m = new KeyedLockManager();
        assertNull(m.acquireReadLockForKey(KEY, "client", "not-a-number"));
    }
}
