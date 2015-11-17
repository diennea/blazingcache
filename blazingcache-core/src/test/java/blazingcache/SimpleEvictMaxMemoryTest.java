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
import blazingcache.server.CacheServer;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class SimpleEvictMaxMemoryTest {

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                assertTrue(client1.waitForConnection(10000));

                client1.put("pippo1", data, 0);
                client1.put("pippo2", data, 0);
                client1.put("pippo3", data, 0);
                client1.put("pippo4", data, 0);
                client1.put("pippo5", data, 0);
                assertEquals(40, client1.getActualMemory());

                Assert.assertArrayEquals(data, client1.get("pippo1").getSerializedData());
                Assert.assertArrayEquals(data, client1.get("pippo2").getSerializedData());
                Assert.assertArrayEquals(data, client1.get("pippo3").getSerializedData());
                Assert.assertArrayEquals(data, client1.get("pippo5").getSerializedData());
                Thread.sleep(100);
                Assert.assertArrayEquals(data, client1.get("pippo4").getSerializedData()); // last get, questo verrà tenuto

                client1.setMaxMemory(10);

                for (int i = 0; i < 100; i++) {
                    System.out.println("client1.getActualMemory():" + client1.getActualMemory());
                    if (client1.getActualMemory() == 8) {
                        break;
                    }
                    Thread.sleep(1000);
                }

                assertEquals(8, client1.getActualMemory());
                assertNull(client1.get("pippo1"));
                assertNull(client1.get("pippo2"));
                assertNull(client1.get("pippo3"));
                assertNull(client1.get("pippo5"));
                Assert.assertArrayEquals(data, client1.get("pippo4").getSerializedData());

            }

        }

    }
}
