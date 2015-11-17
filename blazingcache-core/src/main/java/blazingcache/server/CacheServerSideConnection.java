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
package blazingcache.server;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import blazingcache.network.Channel;
import blazingcache.network.ChannelEventListener;
import blazingcache.network.Message;
import blazingcache.network.ReplyCallback;
import blazingcache.network.ServerSideConnection;

/**
 * Connection to a node from the server side
 *
 * @author enrico.olivelli
 */
public class CacheServerSideConnection implements ChannelEventListener, ServerSideConnection {

    private static final Logger LOGGER = Logger.getLogger(CacheServerSideConnection.class.getName());

    private String clientId;
    private long connectionId;
    private Channel channel;
    private CacheServer server;
    private long lastReceivedMessageTs;

    private static final AtomicLong sessionId = new AtomicLong();

    public CacheServerSideConnection() {
        connectionId = sessionId.incrementAndGet();
    }

    public CacheServer getBroker() {
        return server;
    }

    public void setBroker(CacheServer broker) {
        this.server = broker;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public long getConnectionId() {
        return connectionId;
    }

    String getClientId() {
        return clientId;
    }

    public long getLastReceivedMessageTs() {
        return lastReceivedMessageTs;
    }

    public boolean validate() {
        return channel != null && channel.isValid();
    }

    @Override
    public void messageReceived(Message message) {
        Channel _channel = channel;
        lastReceivedMessageTs = System.currentTimeMillis();
        if (_channel == null) {
            LOGGER.log(Level.SEVERE, "receivedMessageFromWorker {0}, but channel is closed", message);
            return;
        }
        LOGGER.log(Level.FINE, "receivedMessageFromWorker {0}", message);
        switch (message.type) {
            case Message.TYPE_CLIENT_CONNECTION_REQUEST: {
                LOGGER.log(Level.INFO, "connection request {0}", message);
                String sharedSecret = (String) message.parameters.get("secret");
                if (sharedSecret == null || !sharedSecret.equals(server.getSharedSecret())) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("invalid network secret"));
                    return;
                }
                String _clientId = message.clientId;
                if (_clientId == null) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("invalid workerid " + _clientId));
                    return;
                }
                if (!server.isLeader()) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("this broker is not yet writable"));
                    return;
                }
                LOGGER.log(Level.SEVERE, "registering connection " + connectionId + ", workerId:" + _clientId);
                CacheServerSideConnection actual = this.server.getAcceptor().getActualConnectionFromClient(_clientId);
                if (actual != null) {
                    LOGGER.log(Level.SEVERE, "there is already a connection id: {0}, workerId:{1}, {2}", new Object[]{actual.getConnectionId(), _clientId, actual});
                    if (!actual.validate()) {
                        LOGGER.log(Level.SEVERE, "connection id: {0}, is no more valid", actual.getConnectionId());
                        actual.close();
                    } else {
                        answerConnectionNotAcceptedAndClose(message, new Exception("already connected from " + _clientId + ", connectionId " + actual.connectionId + " channel " + actual.channel));
                        return;
                    }
                }

                this.clientId = _clientId;
                server.getAcceptor().connectionAccepted(this);
                answerConnectionAccepted(message);
                break;
            }

            case Message.TYPE_INVALIDATE: {
                String key = (String) message.parameters.get("key");
                server.invalidateKey(key, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;

            }
            case Message.TYPE_UNREGISTER_ENTRY: {
                String key = (String) message.parameters.get("key");
                server.unregisterEntry(key, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;

            }
            case Message.TYPE_FETCH_ENTRY: {
                String key = (String) message.parameters.get("key");
                server.fetchEntry(key, clientId, new SimpleCallback<Message>() {
                    @Override
                    public void onResult(Message result, Throwable error) {
                        _channel.sendReplyMessage(message, result);
                    }
                });
                break;

            }
            case Message.TYPE_INVALIDATE_BY_PREFIX: {
                String prefix = (String) message.parameters.get("prefix");
                server.invalidateByPrefix(prefix, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("prefix", prefix));
                    }
                });
                break;

            }
            case Message.TYPE_PUT_ENTRY: {
                String key = (String) message.parameters.get("key");
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                server.putEntry(key, data, expiretime, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;
            }
            case Message.TYPE_CLIENT_SHUTDOWN:
                LOGGER.log(Level.SEVERE, "worker " + clientId + " sent shutdown message");
                /// ignore
                break;

            default:
                LOGGER.log(Level.SEVERE, "worker " + clientId + " sent unknown message " + message);
                _channel.sendReplyMessage(message, Message.ERROR(clientId, new Exception("invalid message type:" + message.type)));

        }

    }

    @Override
    public void channelClosed() {
        LOGGER.log(Level.SEVERE, "worker " + clientId + " connection " + this + " closed");
        channel = null;
        server.getAcceptor().connectionClosed(this);
        server.clientDisconnected(clientId);
    }

    void answerConnectionNotAcceptedAndClose(Message connectionRequestMessage, Throwable ex
    ) {
        if (channel != null) {
            channel.sendReplyMessage(connectionRequestMessage, Message.ERROR(clientId, ex));
        }
        close();
    }

    public void close() {
        if (channel != null) {
            channel.close();
        } else {
            channelClosed();
        }
    }

    void answerConnectionAccepted(Message connectionRequestMessage
    ) {
        channel.sendReplyMessage(connectionRequestMessage, Message.ACK(clientId));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 43 * hash + (int) (this.connectionId ^ (this.connectionId >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final CacheServerSideConnection other = (CacheServerSideConnection) obj;
        if (this.connectionId != other.connectionId) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheServerSideConnection{" + "clientId=" + clientId + " , connectionId=" + connectionId + ", channel=" + channel + ", lastReceivedMessageTs=" + lastReceivedMessageTs + '}';
    }

    void sendKeyInvalidationMessage(String sourceClientId, String key, BroadcastRequestStatus invalidation) {
        Channel _channel = channel;
        if (_channel == null) {
            // not connected, quindi cache vuota            
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.INVALIDATE(sourceClientId, key), new ReplyCallback() {

            @Override
            public void replyReceived(Message originalMessage, Message message, Throwable error) {
                LOGGER.log(Level.FINEST, clientId + " answered to invalidate " + key + ": " + message + ", " + error);
                // in ogni caso il client ha finito
                invalidation.clientDone(clientId);
            }
        });
    }

    void sendPutEntry(String sourceClientId, String key, byte[] serializedData, long expireTime, BroadcastRequestStatus invalidation) {
        Channel _channel = channel;
        if (_channel == null) {
            // not connected, quindi cache vuota
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.PUT_ENTRY(sourceClientId, key, serializedData, expireTime), new ReplyCallback() {

            @Override
            public void replyReceived(Message originalMessage, Message message, Throwable error) {
                LOGGER.log(Level.FINEST, clientId + " answered to put " + key + ": " + message + ", " + error);
                if (error != null) {
                    error.printStackTrace();
                }
                // in ogni caso il client ha finito
                invalidation.clientDone(clientId);
            }
        });
    }

    void sendPrefixInvalidationMessage(String sourceClientId, String prefix, BroadcastRequestStatus invalidation) {
        Channel _channel = channel;
        if (_channel == null) {
            // not connected, quindi cache vuota
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.INVALIDATE_BY_PREFIX(sourceClientId, prefix), new ReplyCallback() {

            @Override
            public void replyReceived(Message originalMessage, Message message, Throwable error) {
                LOGGER.log(Level.FINEST, clientId + " answered to invalidateByPrefix " + prefix + ": " + message + ", " + error);
                if (error != null) {
                    error.printStackTrace();
                }
                // in ogni caso il client ha finito
                invalidation.clientDone(clientId);
            }
        });
    }

    void sendFetchKeyMessage(String remoteClientId, String key, SimpleCallback<Message> onFinish) {
        Channel _channel = channel;
        if (_channel == null) {
            onFinish.onResult(Message.ERROR(clientId, new Exception("client " + clientId + " disconnected while serving fetch request")), null);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.FETCH_ENTRY(remoteClientId, key), new ReplyCallback() {

            @Override
            public void replyReceived(Message originalMessage, Message message, Throwable error) {
                LOGGER.log(Level.FINEST, remoteClientId + " answered to fetch key " + key + ": " + message + ", " + error);
                if (error != null) {
                    error.printStackTrace();
                }
                if (message != null) {
                    onFinish.onResult(message, null);
                } else {
                    onFinish.onResult(Message.ERROR(clientId, new Exception("client " + clientId + " returned error " + error + " while serving fetch request")), null);
                }
            }
        });
    }

}
