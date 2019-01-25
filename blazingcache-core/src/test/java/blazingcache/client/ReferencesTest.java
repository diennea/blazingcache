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
package blazingcache.client;

import blazingcache.network.ServerHostData;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.server.CacheServer;
import io.netty.util.IllegalReferenceCountException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Test for storing Object references
 *
 * @author enrico.olivelli
 */
public class ReferencesTest {

    private static final AtomicInteger countObjectWrites = new AtomicInteger();
    private static final AtomicInteger countObjectReads = new AtomicInteger();

    @Test
    public void basicTest() throws Exception {

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                    CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));) {
                client1.start();
                client2.start();
                client1.waitForConnection(10000);
                client2.waitForConnection(10000);

                String key = "key1";

                MyBean myObject1 = new MyBean("test1");

                client1.putObject(key, myObject1, -1);

                assertEquals(1, countObjectWrites.get());
                assertEquals(0, countObjectReads.get());

                MyBean reference_to_object1 = client1.getObject(key);
                Assert.assertSame(reference_to_object1, myObject1);

                assertEquals(1, countObjectWrites.get());
                assertEquals(0, countObjectReads.get());

                MyBean other_reference_to_object1 = client1.getObject(key);
                Assert.assertSame(other_reference_to_object1, myObject1);

                assertEquals(1, countObjectWrites.get());
                assertEquals(0, countObjectReads.get());

                client2.putObject(key, myObject1, -1);

                assertEquals(2, countObjectWrites.get());
                assertEquals(0, countObjectReads.get());
                MyBean reference_to_object1_changed = client1.fetchObject(key);
                System.out.println("qui " + reference_to_object1_changed);
                Assert.assertNotSame(reference_to_object1_changed, myObject1);

                assertEquals(2, countObjectWrites.get());
                assertEquals(1, countObjectReads.get());

                System.out.println("qua");
                MyBean reference_to_object1_changed_2 = client1.fetchObject(key);
                Assert.assertSame(reference_to_object1_changed, reference_to_object1_changed_2);

                assertEquals(2, countObjectWrites.get());
                assertEquals(1, countObjectReads.get());

                MyBean reference_to_object1_changed_3 = client1.fetchObject(key);
                Assert.assertSame(reference_to_object1_changed, reference_to_object1_changed_3);

                assertEquals(2, countObjectWrites.get());
                assertEquals(1, countObjectReads.get());

                CacheEntry entry = client1.get(key);
                entry.discardInternalCachedObject();
                entry.close();

                MyBean reference_to_object1_changed_3b = client1.fetchObject(key);
                Assert.assertNotSame(reference_to_object1_changed, reference_to_object1_changed_3b);

                assertEquals(2, countObjectWrites.get());
                assertEquals(2, countObjectReads.get());

                // disconnection clears internal cache and releases all of the references
                // to direct memory
                client1.disconnect();
                assertTrue(client1.waitForConnection(10000));

                assertEquals(0, client1.getCacheSize());
                // old reference to cache entry is not valid any more
                // we can expect a Netty error
                try {
                    entry.getSerializedData();
                    fail("should not work, refcount is now 0");
                } catch (IllegalReferenceCountException expected) {
                }

            }

        }
    }

    public static final class MyBean implements Serializable {

        private String name;

        public MyBean(String name) {
            this.name = name;
        }

        private void writeObject(java.io.ObjectOutputStream out)
                throws IOException {
            out.writeUTF(name);
            countObjectWrites.incrementAndGet();
        }

        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            this.name = in.readUTF();
            countObjectReads.incrementAndGet();
        }

        private void readObjectNoData()
                throws ObjectStreamException {
            countObjectReads.incrementAndGet();
        }
    }
}
