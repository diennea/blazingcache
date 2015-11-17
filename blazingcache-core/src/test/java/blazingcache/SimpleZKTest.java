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
import blazingcache.zookeeper.ZKCacheServerLocator;
import org.junit.Assert;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class SimpleZKTest {

    @Rule
    public TemporaryFolder folderZk = new TemporaryFolder();

    @Test
    public void basicTest() throws Exception {
        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);
        ServerHostData hostData = new ServerHostData("localhost", 1234, "ciao", false, null);
        try (ZKTestEnv zkEnv = new ZKTestEnv(folderZk.getRoot().toPath());
                CacheServer cacheServer = new CacheServer("ciao", hostData)) {
            cacheServer.setupCluster(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath(), hostData);
            cacheServer.start();

            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new ZKCacheServerLocator(zkEnv.getAddress(), zkEnv.getTimeout(), zkEnv.getPath()))) {
                client1.start();
                client2.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));

                client1.put("pippo", data, 0);
                client2.put("pippo", data, 0);

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                client1.invalidate("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));

            }

        }

    }
}
