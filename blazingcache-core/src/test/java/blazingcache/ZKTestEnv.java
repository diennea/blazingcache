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
