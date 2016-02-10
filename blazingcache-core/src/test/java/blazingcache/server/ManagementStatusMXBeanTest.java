/*
 * Copyright 2016 Diennea S.R.L..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package blazingcache.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import blazingcache.client.CacheClientBuilder.Mode;
import blazingcache.management.JMXUtils;
import blazingcache.network.ServerHostData;
import blazingcache.server.CacheServer;

/**
 * Test for Server status MX Bean.
 *
 * @author matteo.casadei
 *
 */
public final class ManagementStatusMXBeanTest {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 1234;
    private static final String CLIENT_NAME = "testClient";
    private static final String SECRET_TEXT = "mymostsecretsecret";
    private static final String STATUS_MBEAN_PATTERN = "blazingcache.server.management:type=CacheServerStatus,CacheServer={0}";
    private static final int CLIENT_CONNECTION_TIMEOUT = 10000;
    private static final byte[] TEST_DATA = "testdata".getBytes(StandardCharsets.UTF_8);
    private static final int MAX_NO_OF_ENTRIES_BEFORE_EVICTION = 20;

    private static CacheServer cacheServer;
    private static ServerHostData serverHostData;

    /**
     * @throws Exception
     *
     */
    @BeforeClass
    public static void setUpEnvironment() throws Exception {
        serverHostData = new ServerHostData(SERVER_HOST, SERVER_PORT, "managementStatusTest", false, null);
        cacheServer = new CacheServer(SECRET_TEXT, serverHostData);
        cacheServer.start();
    }

    /**
     *
     */
    @AfterClass
    public static void cleanUpEnvironment() {
        cacheServer.close();
    }

    /**
     * Check that jmx status bean is unavailable by default.
     *
     * @throws Exception
     */
    @Test(expected = InstanceNotFoundException.class)
    public void statusMBeanDisabled() throws Exception {
        final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, cacheServer.getServerId()));
        JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CurrentTimestamp");
    }
    

    /**
     * Checks that jmx status bean is registered when jmx enabled on
     * ClientCache.
     * <p>
     * Performs also a few tests to make sure status values provided by the
     * mbean are consistent.
     *
     * @throws Exception
     */
    @Test
    public void statusMBeanEnabled() throws Exception {
        try {
            final ObjectName statusBeanName = new ObjectName(MessageFormat.format(STATUS_MBEAN_PATTERN, cacheServer.getServerId()));

            cacheServer.setStatusEnabled(true);
            //wait for mbean to be registered on the platform
//            boolean registrationCompleted = false;
//            for (int i = 0; i < 100; i++) {
//                if (JMXUtils.getMBeanServer().isRegistered(statusBeanName)) {
//                    registrationCompleted = true;
//                    break;
//                }
//                Thread.sleep(10);
//            }
//            assertTrue(registrationCompleted);

            final long beforeCurrentTs = System.currentTimeMillis();
            long currentTs = (Long)JMXUtils.getMBeanServer().getAttribute(statusBeanName, "CurrentTimestamp");
            assertTrue(currentTs >= beforeCurrentTs && currentTs <= System.currentTimeMillis());

            } catch (InstanceNotFoundException ex) {
                ex.printStackTrace();
                fail("BlazingCacheServerStatusMXBean is expected to be present as jmx is enabled");
            }
        }
}
