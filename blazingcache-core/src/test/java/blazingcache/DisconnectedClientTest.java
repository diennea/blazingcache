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
public class DisconnectedClientTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                assertTrue(client1.waitForConnection(10000));

                client1.put("pippo", data, 0);
                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                client1.disconnect();
                assertTrue(client1.waitForDisconnection(60000));

                assertNull(client1.get("pippo"));

            }
        }
    }
}
