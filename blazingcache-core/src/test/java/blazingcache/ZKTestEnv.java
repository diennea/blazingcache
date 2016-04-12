package blazingcache;

import java.nio.file.Path;

import org.apache.curator.test.TestingServer;

public class ZKTestEnv implements AutoCloseable {

    TestingServer zkServer;

    Path path;

    final boolean recoverExpiredSession;

    public ZKTestEnv(final Path path) throws Exception {
        this(path, true);
    }

    public ZKTestEnv(final Path path, final boolean recoverExpiredSession) throws Exception {
        zkServer = new TestingServer(-1, path.toFile(), true);
        this.path = path;
        this.recoverExpiredSession = recoverExpiredSession;
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

    public boolean isRecoverExpiredSession() {
        return recoverExpiredSession;
    }

}
