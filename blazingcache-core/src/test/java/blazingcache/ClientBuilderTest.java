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
package blazingcache;

import java.nio.charset.StandardCharsets;
import blazingcache.client.CacheClient;
import blazingcache.client.CacheClientBuilder;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author enrico.olivelli
 */
public class ClientBuilderTest {

    @Test
    public void basicJvmTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        try (CacheClient client1 = CacheClientBuilder
                .newBuilder()
                .clientId("theClient1")                
                .mode(CacheClientBuilder.Mode.LOCAL)
                .build()) {
            client1.start();
            assertTrue(client1.waitForConnection(10000));

            client1.put("pippo", data, 0);

            Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());

            client1.invalidate("pippo");
            assertNull(client1.get("pippo"));

            client1.put("key2", data, System.currentTimeMillis());
            Thread.sleep(2000);
            assertNull(client1.get("key2"));

            client1.put("key3", data, -1);
            assertNotNull(client1.get("key3"));
            client1.touchEntry("key3", System.currentTimeMillis());
            Thread.sleep(2000);
            assertNull(client1.get("key3"));

        }

    }
}
