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
package blazingcache.jcache;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import org.junit.Test;
import static org.junit.Assert.assertSame;
import org.junit.Before;

/**
 * Test start multiple clients. Each client will start an embedded server
 *
 * @author enrico.olivelli
 */
public class EmbeddedServerTest {

    public static class MyBean implements Serializable {

        public String name;

        public MyBean(String name) {
            this.name = name;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 29 * hash + Objects.hashCode(this.name);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final MyBean other = (MyBean) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            return true;
        }

    }

    private TestingServer zkServer;

    @Before
    public void before() throws Exception {
        zkServer = new TestingServer(true);
    }

    @After
    public void after() throws Exception {
        if (zkServer != null) {
            zkServer.close();
        }
    }

    @Test
    public void testEmbeddedServer() throws Exception {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p_1 = new Properties();
        p_1.setProperty("blazingcache.mode", "server");
        p_1.setProperty("blazingcache.zookeeper.connectstring", zkServer.getConnectString());
        p_1.setProperty("blazingcache.server.port", "7000");

        Properties p_2 = new Properties();
        p_2.setProperty("blazingcache.mode", "server");
        p_2.setProperty("blazingcache.zookeeper.connectstring", zkServer.getConnectString());
        p_2.setProperty("blazingcache.server.port", "7001");

        try (CacheManager cacheManager_1 = cachingProvider
            .getCacheManager(new URI("cacheManager1"), cachingProvider.getDefaultClassLoader(), p_1);
            CacheManager cacheManager_2 = cachingProvider
                .getCacheManager(new URI("cacheManager2"), cachingProvider.getDefaultClassLoader(), p_2)) {

            assertNotSame(cacheManager_1, cacheManager_2);

            MutableConfiguration<String, MyBean> config
                = new MutableConfiguration<String, MyBean>()
                    .setTypes(String.class, MyBean.class)
                    .setStoreByValue(false);

            Cache<String, MyBean> cache_from_1 = cacheManager_1.createCache("simpleCache", config);
            MyBean bean_in_1 = new MyBean("foo");
            cache_from_1.put("foo", bean_in_1);
            assertSame(bean_in_1, cache_from_1.get("foo"));

            Cache<String, MyBean> cache_from_2 = cacheManager_2.createCache("simpleCache", config);
            MyBean bean_from_2 = cache_from_2.get("foo");
            assertEquals(bean_from_2, bean_in_1);

        }
    }
}
