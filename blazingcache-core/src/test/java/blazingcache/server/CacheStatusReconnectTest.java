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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import blazingcache.utils.RawString;
import org.junit.Test;

/**
 * The disconnect cleanup of a client's listeners must be tied to the CONNECTION that
 * registered them, so a late cleanup of a dead connection cannot wipe the registrations a
 * new connection of the same client made after reconnecting.
 *
 * @author diego.salvi
 */
public class CacheStatusReconnectTest {

    private static final RawString K = RawString.of("k");
    private static final RawString K2 = RawString.of("k2");

    @Test
    public void removeClientListenersIsConnectionIdentityAware() {
        CacheStatus status = new CacheStatus();
        status.registerKeyForClient(K, "X", 1L, 0);
        assertTrue(status.getKeysForClient("X").contains(K));

        // a stale cleanup of a DIFFERENT connection id must not remove the registration
        assertEquals(0, status.removeClientListeners("X", 99L));
        assertTrue(status.getKeysForClient("X").contains(K));

        // the owning connection's cleanup removes it
        assertEquals(1, status.removeClientListeners("X", 1L));
        assertTrue(status.getKeysForClient("X").isEmpty());
    }

    @Test
    public void reconnectRegistrationSurvivesOldConnectionCleanup() {
        CacheStatus status = new CacheStatus();
        // old connection 1 registered K
        status.registerKeyForClient(K, "X", 1L, 0);
        // client reconnects as connection 2 and registers K2 (ownership moves to 2)
        status.registerKeyForClient(K2, "X", 2L, 0);

        // the late cleanup of the OLD connection 1 must be a no-op now
        assertEquals(0, status.removeClientListeners("X", 1L));
        assertTrue("the reconnected connection's fresh registration must survive",
                status.getKeysForClient("X").contains(K2));

        // and the current connection's own cleanup still removes everything for the client
        assertFalse(status.getKeysForClient("X").isEmpty());
        status.removeClientListeners("X", 2L);
        assertTrue(status.getKeysForClient("X").isEmpty());
    }

    /**
     * A late registration arriving from an OLDER (already-superseded) connection - e.g. a
     * delayed fetch reply - must not move ownership back to that dead connection: the
     * owner marker advances forward-only. Otherwise the reconnected connection's
     * registrations would become wipeable by the old connection's cleanup.
     */
    @Test
    public void lateRegistrationFromOldConnectionDoesNotRegressOwnership() {
        CacheStatus status = new CacheStatus();
        // the reconnected connection 2 owns the client's registrations
        status.registerKeyForClient(K2, "X", 2L, 0);
        // a delayed registration attributed to the OLD connection 1 lands afterwards
        status.registerKeyForClient(K, "X", 1L, 0);

        // ownership must stay with the newer connection 2: the old connection's cleanup
        // is a no-op and connection 2's live registration survives
        assertEquals(0, status.removeClientListeners("X", 1L));
        assertTrue(status.getKeysForClient("X").contains(K2));

        // connection 2's own cleanup removes everything (including the phantom K)
        status.removeClientListeners("X", 2L);
        assertTrue(status.getKeysForClient("X").isEmpty());
    }
}
