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
package blazingcache.network.jvm;

import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.ConnectionRequestInfo;
import blazingcache.network.Message;
import blazingcache.network.ServerLocator;
import blazingcache.network.ServerNotAvailableException;
import blazingcache.network.ServerRejectedConnectionException;
import blazingcache.server.CacheServer;

import java.util.concurrent.TimeoutException;

/**
 * Connects to the broker inside the same JVM (for tests)
 *
 * @author enrico.olivelli
 */
public class JVMBrokerLocator implements ServerLocator {

    private final CacheServer broker;
    private final String brokerId;
    private JVMChannel workerSide;

    public JVMBrokerLocator(String brokerId, CacheServer broker) {
        this.brokerId = brokerId;
        this.broker = broker;
    }

    @Override
    public Channel connect(ChannelEventListener worker, ConnectionRequestInfo workerInfo) throws InterruptedException, ServerRejectedConnectionException, ServerNotAvailableException {
        if (broker == null || !broker.isLeader()) {
            throw new ServerNotAvailableException(new Exception("embedded server " + brokerId + " is not running"));
        }
        workerSide = new JVMChannel();
        workerSide.setMessagesReceiver(worker);
        JVMChannel brokerSide = new JVMChannel();
        broker.getAcceptor().createConnection(brokerSide);
        brokerSide.setOtherSide(workerSide);
        workerSide.setOtherSide(brokerSide);
        Message acceptMessage = Message.CLIENT_CONNECTION_REQUEST(workerInfo.getClientId(), workerInfo.getSharedSecret());
        try {
            Message connectionResponse = workerSide.sendMessageWithReply(acceptMessage, 10000);
            if (connectionResponse.type == Message.TYPE_ACK) {
                return workerSide;
            } else {
                throw new ServerRejectedConnectionException("Server rejected connection, response message:" + connectionResponse);
            }
        } catch (TimeoutException err) {
            throw new ServerNotAvailableException(err);
        }

    }

    @Override
    public void brokerDisconnected() {
    }

    @Override
    public void close() {
        if (workerSide != null) {
            workerSide.close();
        }
    }

}
