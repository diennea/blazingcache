package blazingcache;

import java.util.Random;
import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.ConnectionRequestInfo;
import blazingcache.network.Message;
import blazingcache.network.ServerSideConnection;
import blazingcache.network.ServerSideConnectionAcceptor;
import blazingcache.network.netty.NettyCacheServerLocator;
import blazingcache.network.netty.NettyChannelAcceptor;
import org.junit.Test;

public class ChannelBenchTest {

    @Test
    public void test() throws Exception {
        try (NettyChannelAcceptor acceptor = new NettyChannelAcceptor("localhost", 1111, false)) {
            acceptor.setAcceptor(new ServerSideConnectionAcceptor() {

                @Override
                public ServerSideConnection createConnection(Channel channel) {
                    channel.setMessagesReceiver(new ChannelEventListener() {

                        @Override
                        public void messageReceived(Message message) {
                            channel.sendReplyMessage(message, Message.ACK("ciao"));
                        }

                        @Override
                        public void channelClosed() {

                        }
                    });
                    return new ServerSideConnection() {
                        @Override
                        public long getConnectionId() {
                            return new Random().nextLong();
                        }
                    };
                }
            });
            acceptor.start();
            try (NettyCacheServerLocator locator = new NettyCacheServerLocator("localhost", 1111, false)) {
                try (Channel client = locator.connect(new ChannelEventListener() {

                    @Override
                    public void messageReceived(Message message) {
                        System.out.println("client messageReceived " + message);
                    }

                    @Override
                    public void channelClosed() {
                        System.out.println("client channelClosed");

                    }
                }, new ConnectionRequestInfo() {

                    @Override
                    public String getClientId() {
                        return "myclient";
                    }

                    @Override
                    public String getSharedSecret() {
                        return "secret";
                    }
                });) {
                    for (int i = 0; i < 100; i++) {
                        Message result = client.sendMessageWithReply(Message.ACK("clientId"), 10000);
                        System.out.println("result:" + result);
                    }
                }
            }
        }
    }
}
