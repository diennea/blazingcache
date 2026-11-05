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
package blazingcache.server;

import static org.junit.Assert.assertEquals;
import blazingcache.network.ServerHostData;
import org.junit.Test;

/**
 * A non positive period would kill (or spin) the server background threads.
 */
public class CacheServerPeriodsConfigurationTest {

    private static CacheServer newServer() {
        return new CacheServer("ciao", new ServerHostData("localhost", 1234, "test", false, null));
    }

    @Test
    public void validPeriods() {
        CacheServer server = newServer();
        server.setExpirerPeriod(1);
        server.setIdleConnectionsCheckPeriod(1);
        assertEquals(1, server.getExpirerPeriod());
        assertEquals(1, server.getIdleConnectionsCheckPeriod());
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroExpirerPeriod() {
        newServer().setExpirerPeriod(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeExpirerPeriod() {
        newServer().setExpirerPeriod(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroIdleConnectionsCheckPeriod() {
        newServer().setIdleConnectionsCheckPeriod(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeIdleConnectionsCheckPeriod() {
        newServer().setIdleConnectionsCheckPeriod(-1);
    }
}
