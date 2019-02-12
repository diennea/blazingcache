/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package blazingcache;

import java.nio.charset.StandardCharsets;
import blazingcache.client.CacheClient;
import blazingcache.client.EntryHandle;
import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author enrico.olivelli
 */
public class FetchTest {

//     @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.FINEST;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter();
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }
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

                EntryHandle _remoteLoaded;
                try (EntryHandle remoteLoad = client2.fetch("pippo");) {
                    assertNotNull(remoteLoad);
                    Assert.assertArrayEquals(data, remoteLoad.getSerializedData());
                    _remoteLoaded = remoteLoad;
                }

                // same fetch, hits local cache
                try (EntryHandle remoteLoad = client2.fetch("pippo");) {
                    assertSame(remoteLoad, _remoteLoaded);
                    Assert.assertArrayEquals(data, remoteLoad.getSerializedData());
                }

                Assert.assertArrayEquals(data, client1.get("pippo").getSerializedData());
                Assert.assertArrayEquals(data, client2.get("pippo").getSerializedData());

                client1.invalidate("pippo");
                assertNull(client1.get("pippo"));
                assertNull(client2.get("pippo"));

            }

        }

    }
}
