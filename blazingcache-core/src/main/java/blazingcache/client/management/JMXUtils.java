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
package blazingcache.client.management;

import java.util.logging.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import blazingcache.client.CacheClient;

/**
 * Utility for MBeans registration.
 *
 * @author matteo.casadei
 */
public final class JMXUtils {

    private static final Logger LOGGER = Logger.getLogger(JMXUtils.class.getName());

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

    /**
     *
     */
    private JMXUtils() {
    }

    /**
     * Utility method adopted to replace forbidden chars in MBean name with "."
     * character.
     *
     * @param text
     *            the text to "sanitize"
     * @return a sanitized version of s, where any occurrence of ",", ":", "=",
     *         "\n", "\r" is replaced by ".".
     */
    private static String safeName(final String text) {
        return text.replaceAll(",|:|=|\n\r#", ".");
    }

    /**
     * Return this JVM default MBeanServer.
     *
     * @return the default MBeanServer
     */
    public static MBeanServer getMBeanServer() {
        return platformMBeanServer;
    }

    /**
     * Register the statistics MBean for the specified client cache on the platform mbean server.
     *
     * @param client the cache client on which statistics of the mbean refer to
     * @param bean the mbean providing cache client statistics
     */
    public static void registerClientStatisticsMXBean(final CacheClient client, final CacheClientStatisticsMXBean bean) {
        if (platformMBeanServer == null) {
            throw new CacheClientManagementException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        final String cacheClientId = safeName(client.getClientId());

        try {
            final ObjectName name = new ObjectName("blazingcache.client.management:type=CacheClientStatistics,CacheClient=" + cacheClientId);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                    LOGGER.warning("Impossible to unregister non-registered mbean: " + name + ". Cause: " + noProblem);
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new CacheClientManagementException("Could not register MXBean ", e);
        }
    }

    /**
     * Unregister the mbean providing the statistics related to the specified {@link CacheClient}.
     *
     * @param client the client on which statistics mbean has to be unregistered
     */
    public static void unregisterClientStatisticsMXBean(final CacheClient client) {
        if (platformMBeanServer == null) {
            return;
        }
        final String cacheClientId = safeName(client.getClientId());

        try {
            ObjectName name = new ObjectName("blazingcache.client.management:type=CacheClientStatistics,CacheClient=" + cacheClientId);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                    LOGGER.warning("Impossible to unregister non-registered mbean: " + name + ". Cause: " + noProblem);
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new CacheClientManagementException("Could not register MXBean ", e);
        }
    }

    /**
     * Register the status MBean for the specified client cache on the platform mbean server.
     *
     * @param client the cache client on which status provided by the mbean refer to
     * @param bean the mbean providing cache client status
     */
    public static void registerClientStatusMXBean(final CacheClient client, final CacheClientStatusMXBean bean) {
        if (platformMBeanServer == null) {
            throw new CacheClientManagementException("PlatformMBeanServer not available", mBeanServerLookupError);
        }
        final String cacheClientId = safeName(client.getClientId());

        try {
            ObjectName name = new ObjectName("blazingcache.client.management:type=CacheClientStatus,CacheClient=" + cacheClientId);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                    LOGGER.warning("Impossible to unregister non-registered mbean: " + name + ". Cause: " + noProblem);
                }
            }
            platformMBeanServer.registerMBean(bean, name);
        } catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
            throw new CacheClientManagementException("Could not register MXBean " + e);
        }
    }

    /**
     * Unregister the mbean providing the status related to the specified {@link CacheClient}.
     *
     * @param client the client on which status mbean has to be unregistered
     */
    public static void unregisterClientStatusMXBean(final CacheClient client) {
        if (platformMBeanServer == null) {
            return;
        }
        final String cacheClientId = safeName(client.getClientId());

        try {
            ObjectName name = new ObjectName("blazingcache.client.management:type=CacheClientStatus,CacheClient=" + cacheClientId);

            if (platformMBeanServer.isRegistered(name)) {
                try {
                    platformMBeanServer.unregisterMBean(name);
                } catch (InstanceNotFoundException noProblem) {
                    LOGGER.warning("Impossible to unregister non-registered mbean: " + name + ". Cause: " + noProblem);
                }
            }
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            throw new CacheClientManagementException("Could not register MXBean ", e);
        }
    }
}
