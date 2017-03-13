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
import java.io.File;
import java.io.FileWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;
import javax.security.auth.login.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

/**
 * Test for loadEntry
 *
 * @author enrico.olivelli
 */
public class JAASKerberosTest {

    private MiniKdc kdc;
    private Properties conf;

    @Rule
    public TemporaryFolder kdcDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder kerberosWorkDir = new TemporaryFolder();

    @Before
    public void startMiniKdc() throws Exception {

        createMiniKdcConf();
        kdc = new MiniKdc(conf, kdcDir.getRoot());
        kdc.start();

        String localhostName = "localhost.localdomain";
        InetAddress byName = InetAddress.getByName(localhostName);
        System.err.println("debug InetAddress of "+localhostName+" is "+byName);
        System.err.println("InetAddress of "+localhostName+" is "+byName.getHostAddress());
        System.err.println("InetAddress of "+localhostName+" is "+byName.getCanonicalHostName());
        System.err.println("InetAddress of "+localhostName+" is "+byName.getHostName());
        String principalServerNoRealm = "blazingcache/" + localhostName;
        String principalServer = "blazingcache/" + localhostName + "@" + kdc.getRealm();
        String principalClientNoRealm = "blazingcacheclient/" + localhostName;
        String principalClient = principalClientNoRealm + "@" + kdc.getRealm();

        System.out.println("adding principal: " + principalServerNoRealm);
        System.out.println("adding principal: " + principalClientNoRealm+" -> "+byName);

        File keytabClient = new File(kerberosWorkDir.getRoot(), "blazingcacheclient.keytab");
        kdc.createPrincipal(keytabClient, principalClientNoRealm);

        File keytabServer = new File(kerberosWorkDir.getRoot(), "blazingcacheserver.keytab");
        kdc.createPrincipal(keytabServer, principalServerNoRealm);

        File jaas_file = new File(kerberosWorkDir.getRoot(), "jaas.conf");
        try (FileWriter writer = new FileWriter(jaas_file)) {
            writer.write("\n"
                + "BlazingCacheServer {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabServer.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalServer + "\";\n"
                + "};\n"
                + "\n"
                + "\n"
                + "\n"
                + "BlazingCacheClient {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabClient.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalClient + "\";\n"
                + "};\n"
            );

        }

        File krb5file = new File(kerberosWorkDir.getRoot(), "krb5.conf");
        try (FileWriter writer = new FileWriter(krb5file)) {
            writer.write("[libdefaults]\n"
                + " default_realm = " + kdc.getRealm() + "\n"
                + "\n"
                + "\n"
                + "[realms]\n"
                + " " + kdc.getRealm() + "  = {\n"
                + "  kdc = " + kdc.getHost() + ":" + kdc.getPort() + "\n"
                + " }"
            );

        }

        System.setProperty("java.security.auth.login.config", jaas_file.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5file.getAbsolutePath());
        System.setProperty("sun.security.krb5.debug", "true");
        javax.security.auth.login.Configuration.getConfiguration().refresh();

    }

    /**
     *
     * /**
     * Create a Kdc configuration
     */
    public void createMiniKdcConf() {
        conf = MiniKdc.createConf();
    }

    @After
    public void stopMiniKdc() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("java.security.krb5.conf");
        if (kdc != null) {
            kdc.stop();
        }
    }

    @Test
    public void basicTest() throws Exception {

        byte[] data = "testdata".getBytes(StandardCharsets.UTF_8);

        ServerHostData serverHostData = new ServerHostData("localhost", 1234, "test", false, null);
        try (CacheServer cacheServer = new CacheServer("ciao", serverHostData)) {
            cacheServer.setClientFetchTimeout(1000);
            cacheServer.start();
            try (CacheClient client1 = new CacheClient("theClient1", "ciao", new NettyCacheServerLocator(serverHostData));
                CacheClient client2 = new CacheClient("theClient2", "ciao", new NettyCacheServerLocator(serverHostData));
                CacheClient client3 = new CacheClient("theClient3", "ciao", new NettyCacheServerLocator(serverHostData));) {

                client1.start();
                client2.start();
                client3.start();

                assertTrue(client1.waitForConnection(10000));
                assertTrue(client2.waitForConnection(10000));
                assertTrue(client3.waitForConnection(10000));

                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

                client1.load("foo", data, 0);
                assertNotNull(client2.fetch("foo"));

                client3.invalidate("foo");
                assertNull(client1.get("foo"));
                assertNull(client2.get("foo"));

            }

        }

    }

}
