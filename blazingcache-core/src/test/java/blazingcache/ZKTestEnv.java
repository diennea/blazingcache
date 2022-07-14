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
package blazingcache;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;

public class ZKTestEnv implements AutoCloseable {

    final TestingServer zkServer;

    public ZKTestEnv(final Path path) throws Exception {
        Map<String, Object> customProperties = new HashMap<>();
        customProperties.put("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        customProperties.put("kerberos.removeHostFromPrincipal", "true");
        customProperties.put("kerberos.removeRealmFromPrincipal", "true");
        customProperties.put("syncEnabled", "false");
        InstanceSpec spec = new InstanceSpec(path.toFile(), 1111, 2222, 2223, false, 1, 1000, 100,
                customProperties, "localhost");
        zkServer = new TestingServer(spec, false);
        zkServer.start();
    }

    public String getAddress() {
        return zkServer.getConnectString();
    }

    public int getTimeout() {
        return 40000;
    }

    public String getPath() {
        return "/simplecachetest";
    }

    @Override
    public void close() throws Exception {
        try {
            if (zkServer != null) {
                zkServer.close();
            }
        } catch (Throwable t) {
        }
    }
}
