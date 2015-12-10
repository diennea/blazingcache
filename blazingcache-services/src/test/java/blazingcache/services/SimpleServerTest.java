/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package blazingcache.services;

import blazingcache.client.CacheClient;
import blazingcache.client.CacheEntry;
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
            try (CacheClient client = new CacheClient("test", "blazing", new NettyCacheServerLocator("localhost", 1025, true));) {

                client.start();
                assertTrue(client.waitForConnection(10000));
                client.put("test", "ciao".getBytes(), -1);
                CacheEntry entry = client.get("test");
                Assert.assertNotNull(entry);
                Assert.assertArrayEquals("ciao".getBytes(), entry.getSerializedData());
            }

        }

    }

}
