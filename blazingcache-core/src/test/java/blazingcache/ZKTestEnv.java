package blazingcache;

import java.nio.file.Path;

public class ZKTestEnv implements AutoCloseable {

    TestingZookeeperServerEmbedded zkServer;

    Path path;

    public ZKTestEnv(final Path path) throws Exception {
        zkServer = new TestingZookeeperServerEmbedded(1281, path.toFile());
        zkServer.start();
        this.path = path;
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
