/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package blazingcache;

import java.nio.charset.StandardCharsets;
import blazingcache.client.CacheClient;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.network.netty.NettyChannelAcceptor;
import blazingcache.server.CacheServer;
import org.junit.Assert;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class ExpireTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "testdata2".getBytes(StandardCharsets.UTF_8);
        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                client2.put("pippo", data2, System.currentTimeMillis() + 20000);
                Assert.assertArrayEquals(data2, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data2, client2.get("pippo").getSerializedData());

                for (int i = 0; i < 60; i++) {
                    boolean ok = client1.get("pippo") == null && client2.get("pippo") == null;
                    Thread.sleep(1000);
                    if (ok) {
                        break;
                    }
                }

                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));

            }

        }
    }

}
