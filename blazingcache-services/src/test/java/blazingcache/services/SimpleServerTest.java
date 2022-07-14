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
package blazingcache.services;

import blazingcache.client.CacheClient;
import blazingcache.client.EntryHandle;
import blazingcache.network.netty.NettyCacheServerLocator;
import java.util.Properties;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SimpleServerTest {

    @Test
    public void test() throws Exception {
        Properties pp = new Properties();

        try (ServerMain main = new ServerMain(pp);) {
            main.start();
            try (CacheClient client = new CacheClient("test", "blazingcache", new NettyCacheServerLocator("localhost", 1025, false));) {
                client.start();
                assertTrue(client.waitForConnection(10000));
                client.put("test", "ciao".getBytes(), -1);
                try (EntryHandle entry = client.get("test");) {
                    Assert.assertNotNull(entry);
                    Assert.assertArrayEquals("ciao".getBytes(), entry.getSerializedData());
                }
            }

        }

    }

}
