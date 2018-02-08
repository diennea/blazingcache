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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.AccessedExpiryPolicy;
import static javax.cache.expiry.Duration.ONE_HOUR;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import org.junit.Test;
import static org.junit.Assert.assertSame;

/**
 * Test storeByReference
 *
 * @author enrico.olivelli
 */
public class StoreByReferenceSimpleTest {

    public static class MyBean implements Serializable {

        public String name;

        public MyBean(String name) {
            this.name = name;
        }

    }

    @Test
    public void testStoreByReference() {

        CachingProvider cachingProvider = Caching.getCachingProvider();
        Properties p = new Properties();
        try (CacheManager cacheManager = cachingProvider.getCacheManager(cachingProvider.getDefaultURI(), cachingProvider.getDefaultClassLoader(), p)) {
            MutableConfiguration<String, MyBean> config
                    = new MutableConfiguration<String, MyBean>()
                    .setTypes(String.class, MyBean.class)
                    .setStoreByValue(false)
                    .setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(ONE_HOUR))
                    .setStatisticsEnabled(true);

            Cache<String, MyBean> cache = cacheManager.createCache("simpleCache", config);
            MyBean bean1 = new MyBean("foo");
            cache.put("foo", bean1);
            assertSame(bean1, cache.get("foo"));

            MyBean bean_one = new MyBean("one");
            MyBean bean_two = new MyBean("two");

            Map<String, MyBean> all = new HashMap<>();
            all.put("one", bean_one);
            all.put("two", bean_two);
            cache.putAll(all);

            Map<String, MyBean> result = cache.getAll(new HashSet<>(Arrays.asList("one", "two")));
            assertSame(bean_one, cache.get("one"));
            assertSame(bean_two, cache.get("two"));
            assertSame(bean_one, result.get("one"));
            assertSame(bean_two, result.get("two"));

            AtomicReference<MyBean> result_in_invoke = new AtomicReference<>();
            cache.invoke("one", new EntryProcessor<String, StoreByReferenceSimpleTest.MyBean, Object>() {
                @Override
                public Object process(MutableEntry<String, MyBean> entry, Object... arguments) throws EntryProcessorException {
                    result_in_invoke.set(entry.getValue());
                    return null;
                }
            });
            assertSame(bean_one, result_in_invoke.get());

            AtomicReference<MyBean> result_in_invoke_all = new AtomicReference<>();
            cache.invokeAll(new HashSet<>(Arrays.asList("one")), new EntryProcessor<String, StoreByReferenceSimpleTest.MyBean, Object>() {
                @Override
                public Object process(MutableEntry<String, MyBean> entry, Object... arguments) throws EntryProcessorException {
                    result_in_invoke_all.set(entry.getValue());
                    return null;
                }
            });
            assertSame(bean_one, result_in_invoke.get());
        }
    }
}
