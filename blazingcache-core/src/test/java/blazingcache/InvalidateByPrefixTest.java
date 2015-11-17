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
public class InvalidateByPrefixTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
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
                client1.put("pippo-1", data, 0);
                client2.put("pippo", data, 0);
                client2.put("pippo-2", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client1.get("pippo-1").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo-2").getSerializedData());

                client1.invalidateByPrefix("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client1.get("pippo-1"));
                assertNull(client2.get("pippo"));
                assertNull(client2.get("pippo-2"));

            }

        }
    }
}
