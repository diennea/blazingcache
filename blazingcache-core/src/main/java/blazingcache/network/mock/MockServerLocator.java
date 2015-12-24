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
package blazingcache.network.mock;

import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.ConnectionRequestInfo;
import blazingcache.network.Message;
import blazingcache.network.ReplyCallback;
import blazingcache.network.SendResultCallback;
import blazingcache.network.ServerLocator;
import blazingcache.network.ServerNotAvailableException;
import blazingcache.network.ServerRejectedConnectionException;
import java.util.concurrent.TimeoutException;

/**
 * Mock Server Implementation, for JVM-only clients
 *
 * @author enrico.olivelli
 */
public class MockServerLocator implements ServerLocator {

    @Override
    public Channel connect(ChannelEventListener messageReceiver, ConnectionRequestInfo workerInfo) throws InterruptedException, ServerNotAvailableException, ServerRejectedConnectionException {
        return new Channel() {
            @Override
            public void sendOneWayMessage(Message message, SendResultCallback callback) {
            }

            @Override
            public void sendReplyMessage(Message inAnswerTo, Message message) {
            }

            @Override
            public Message sendMessageWithReply(Message message, long timeout) throws InterruptedException, TimeoutException {
                switch (message.type) {
                    case Message.TYPE_FETCH_ENTRY:
                        return Message.ERROR("mock", new Exception("no entry"));
                    case Message.TYPE_INVALIDATE:
                        return Message.ACK("mock");
                    case Message.TYPE_INVALIDATE_BY_PREFIX:
                        return Message.ACK("mock");
                    case Message.TYPE_PUT_ENTRY:
                        return Message.ACK("mock");
                    default:
                        throw new RuntimeException("not yet supported but mock");
                }
            }

            @Override
            public void sendMessageWithAsyncReply(Message message, ReplyCallback callback) {
                switch (message.type) {
                    case Message.TYPE_UNREGISTER_ENTRY:
                        callback.replyReceived(message, Message.ACK("mock"), null);
                        break;
                    default:
                        throw new RuntimeException("not yet supported but mock");
                }
            }

            @Override
            public void close() {

            }

            @Override
            public boolean isValid() {
                return true;
            }
        };
    }

    @Override

    public void brokerDisconnected() {

    }

    @Override
    public void close() {
    }

}
