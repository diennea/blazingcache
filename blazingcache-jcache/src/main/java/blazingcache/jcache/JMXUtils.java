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

import blazingcache.jcache.BlazingCacheCache;
import java.lang.management.ManagementFactory;
import javax.cache.CacheException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 * Utility for MBeans registration
 *
 * @author enrico.olivelli
 */
public class JMXUtils {

    private static MBeanServer platformMBeanServer;
    private static Throwable mBeanServerLookupError;

    static {
        try {
            platformMBeanServer = MBeanServerFactory.createMBeanServer();
        } catch (Exception err) {
            mBeanServerLookupError = err;
            err.printStackTrace();
            platformMBeanServer = null;
        }
    }

    private static String safeName(String s) {
        return s.replaceAll("[?,:=\\W]", ".");
    }

    public static MBeanServer getMBeanServer() {
        return platformMBeanServer;
    }

    public static <K, V> void registerStatisticsMXBean(BlazingCacheCache<K, V> cache, BlazingCacheStatisticsMXBean<K, V> bean) {
        if (platformMBeanServer == null) {
            throw new CacheException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        String cacheManagerName = safeName(cache.getCacheManager().getURI().toString());        
        String cacheName = safeName(cache.getName());        

        try {
            ObjectName name = new ObjectName("javax.cache:type=CacheStatistics,CacheManager=" + cacheManagerName + ",Cache=" + cacheName);            

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new CacheException("Could not register MXBean " + e);
        }
    }

    public static <K, V> void unregisterStatisticsMXBean(BlazingCacheCache<K, V> cache) {
        if (platformMBeanServer == null) {
            return;
        }
        String cacheManagerName = safeName(cache.getCacheManager().getURI().toString());
        String cacheName = safeName(cache.getName());

        try {
            ObjectName name = new ObjectName("javax.cache:type=CacheStatistics,CacheManager=" + cacheManagerName + ",Cache=" + cacheName);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new CacheException("Could not register MXBean " + e);
        }
    }

    public static <K, V> void registerConfigurationMXBean(BlazingCacheCache<K, V> cache, BlazingCacheConfigurationMXBean<K, V> bean) {
        if (platformMBeanServer == null) {
            throw new CacheException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        String cacheManagerName = safeName(cache.getCacheManager().getURI().toString());
        String cacheName = safeName(cache.getName());

        try {
            ObjectName name = new ObjectName("javax.cache:type=CacheConfiguration,CacheManager=" + cacheManagerName + ",Cache=" + cacheName);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new CacheException("Could not register MXBean " + e);
        }
    }

    public static <K, V> void unregisterConfigurationMXBean(BlazingCacheCache<K, V> cache) {
        if (platformMBeanServer == null) {
            return;
        }
        String cacheManagerName = safeName(cache.getCacheManager().getURI().toString());
        String cacheName = safeName(cache.getName());

        try {
            ObjectName name = new ObjectName("javax.cache:type=CacheConfiguration,CacheManager=" + cacheManagerName + ",Cache=" + cacheName);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new CacheException("Could not register MXBean " + e);
        }
    }
}
