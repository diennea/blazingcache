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
import blazingcache.network.HashUtils;
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
    private int fetchPriority;
    private long connectionId;
    private Channel channel;
    private CacheServer server;
    private long lastReceivedMessageTs;
    private final long MAX_TS_DELTA = Long.getLong("blazingcache.server.maxclienttsdelta", 1000L * 60 * 60);

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
        Channel _channel = channel;
        return _channel != null && _channel.isValid();
    }

    @Override
    public void messageReceived(Message message) {
        Channel _channel = channel;
        lastReceivedMessageTs = System.currentTimeMillis();
        if (_channel == null) {
            LOGGER.log(Level.SEVERE, "receivedMessage {0}, but channel is closed", message);
            return;
        }
        LOGGER.log(Level.FINER, "receivedMessageFromWorker {0}", message);
        switch (message.type) {
            case Message.TYPE_CLIENT_CONNECTION_REQUEST: {
                LOGGER.log(Level.INFO, "connection request from {0}", message.clientId);
                String challenge = (String) message.parameters.get("challenge");
                String ts = (String) message.parameters.get("ts");
                int fetchPriority = 10;
                if (message.parameters.containsKey("fetchPriority")) {
                    fetchPriority = Integer.parseInt(message.parameters.get("fetchPriority") + "");
                }
                if (challenge != null && ts != null) {
                    String expectedChallenge = HashUtils.sha1(ts + "#" + server.getSharedSecret());
                    if (!challenge.equals(expectedChallenge)) {
                        answerConnectionNotAcceptedAndClose(message, new Exception("invalid network challenge"));
                        return;
                    }
                    long _ts = 0;
                    try {
                        _ts = Long.parseLong(ts);
                    } catch (NumberFormatException ee) {
                    }
                    long now = System.currentTimeMillis();
                    long delta = Math.abs(now - _ts);
                    if (delta > MAX_TS_DELTA) {
                        LOGGER.log(Level.INFO, "connection request from {0} -> invalid network challenge. client/server clocks are not in sync now=" + new java.sql.Timestamp(now) + " client time:" + new java.sql.Timestamp(_ts), message.clientId);
                        answerConnectionNotAcceptedAndClose(message, new Exception("invalid network challenge. client/server clocks are not in sync now=" + new java.sql.Timestamp(now) + " client time:" + new java.sql.Timestamp(_ts)));
                        return;
                    }
                } else {
                    // legacy 1.1.x clients
                    String sharedSecret = (String) message.parameters.get("secret");
                    if (sharedSecret == null || !sharedSecret.equals(server.getSharedSecret())) {
                        answerConnectionNotAcceptedAndClose(message, new Exception("invalid network secret"));
                        return;
                    }
                }
                String _clientId = message.clientId;
                if (_clientId == null) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("invalid clientid " + _clientId));
                    return;
                }
                if (!server.isLeader()) {
                    answerConnectionNotAcceptedAndClose(message, new Exception("this broker is not yet writable"));
                    return;
                }
                LOGGER.log(Level.SEVERE, "registering connection " + connectionId + ", clientId:" + _clientId);
                CacheServerSideConnection actual = this.server.getAcceptor().getActualConnectionFromClient(_clientId);
                if (actual != null) {
                    LOGGER.log(Level.SEVERE, "there is already a connection id: {0}, clientId:{1}, {2}", new Object[]{actual.getConnectionId(), _clientId, actual});
                    if (!actual.validate()) {
                        LOGGER.log(Level.SEVERE, "connection id: {0}, is no more valid", actual.getConnectionId());
                        actual.close();
                    } else {
                        answerConnectionNotAcceptedAndClose(message, new Exception("already connected from " + _clientId + ", connectionId " + actual.connectionId + " channel " + actual.channel));
                        return;
                    }
                }
                this.fetchPriority = fetchPriority;
                this.clientId = _clientId;
                server.getAcceptor().connectionAccepted(this);
                answerConnectionAccepted(message);
                this.server.addConnectedClients(1);
                break;
            }

            case Message.TYPE_LOCK_ENTRY: {
                String key = (String) message.parameters.get("key");
                server.addPendingOperations(1);
                server.lockKey(key, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key).setParameter("lockId", result));
                    }
                });
                break;
            }
            case Message.TYPE_UNLOCK_ENTRY: {
                String key = (String) message.parameters.get("key");
                String lockId = (String) message.parameters.get("lockId");
                server.addPendingOperations(1);
                server.unlockKey(key, clientId, lockId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key).setParameter("lockId", result));
                    }
                });
                break;
            }

            case Message.TYPE_INVALIDATE: {
                String key = (String) message.parameters.get("key");
                String lockId = (String) message.parameters.get("lockId");
                server.addPendingOperations(1);
                server.invalidateKey(key, clientId, lockId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;

            }
            case Message.TYPE_UNREGISTER_ENTRY: {
                String key = (String) message.parameters.get("key");
                server.addPendingOperations(1);
                server.unregisterEntry(key, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;

            }
            case Message.TYPE_FETCH_ENTRY: {
                String key = (String) message.parameters.get("key");
                String lockId = (String) message.parameters.get("lockId");
                server.addPendingOperations(1);
                server.fetchEntry(key, clientId, lockId, new SimpleCallback<Message>() {
                    @Override
                    public void onResult(Message result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, result);
                    }
                });
                break;

            }

            case Message.TYPE_TOUCH_ENTRY: {
                String key = (String) message.parameters.get("key");
                long expiretime = (long) message.parameters.get("expiretime");
                server.addPendingOperations(1);
                server.touchEntry(key, clientId, expiretime);
                server.addPendingOperations(-1);
                break;

            }
            case Message.TYPE_INVALIDATE_BY_PREFIX: {
                String prefix = (String) message.parameters.get("prefix");
                server.addPendingOperations(1);
                server.invalidateByPrefix(prefix, clientId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("prefix", prefix));
                    }
                });
                break;

            }
            case Message.TYPE_PUT_ENTRY: {
                String key = (String) message.parameters.get("key");
                byte[] data = (byte[]) message.parameters.get("data");
                long expiretime = (long) message.parameters.get("expiretime");
                String lockId = (String) message.parameters.get("lockId");
                server.addPendingOperations(1);
                server.putEntry(key, data, expiretime, clientId, lockId, new SimpleCallback<String>() {
                    @Override
                    public void onResult(String result, Throwable error) {
                        server.addPendingOperations(-1);
                        _channel.sendReplyMessage(message, Message.ACK(null).setParameter("key", key));
                    }
                });
                break;
            }
            case Message.TYPE_CLIENT_SHUTDOWN:
                LOGGER.log(Level.SEVERE, "client " + clientId + " sent shutdown message");
                this.server.addConnectedClients(-1);
                /// ignore
                break;

            default:
                LOGGER.log(Level.SEVERE, "client " + clientId + " sent unknown message " + message);
                _channel.sendReplyMessage(message, Message.ERROR(clientId, new Exception("invalid message type:" + message.type)));

        }

    }

    @Override
    public void channelClosed() {
        LOGGER.log(Level.SEVERE, "client " + clientId + " connection closed " + this);
        Channel _channel = channel;
        if (_channel != null) {
            _channel.close();
        }
        channel = null;
        server.getAcceptor().connectionClosed(this);
        server.clientDisconnected(clientId);
    }

    void answerConnectionNotAcceptedAndClose(Message connectionRequestMessage, Throwable ex
    ) {
        Channel _channel = channel;
        if (_channel != null) {
            _channel.sendReplyMessage(connectionRequestMessage, Message.ERROR(clientId, ex));
        }
        close();
    }

    public void close() {
        Channel _channel = channel;
        if (_channel != null) {
            _channel.close();
        } else {
            channelClosed();
        }
    }

    void answerConnectionAccepted(Message connectionRequestMessage
    ) {
        Channel _channel = channel;
        if (_channel != null) {
            _channel.sendReplyMessage(connectionRequestMessage, Message.ACK(clientId));
        }
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
        if (_channel == null || !_channel.isValid()) {
            // not connected, quindi cache vuota            
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.INVALIDATE(sourceClientId, key), server.getSlowClientTimeout(), new ReplyCallback() {

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
        if (_channel == null || !_channel.isValid()) {
            // not connected, quindi cache vuota
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.PUT_ENTRY(sourceClientId, key, serializedData, expireTime), server.getSlowClientTimeout(), new ReplyCallback() {

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
        if (_channel == null || !_channel.isValid()) {
            // not connected, quindi cache vuota
            invalidation.clientDone(clientId);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.INVALIDATE_BY_PREFIX(sourceClientId, prefix), server.getSlowClientTimeout(), new ReplyCallback() {

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
        if (_channel == null || !_channel.isValid()) {
            onFinish.onResult(Message.ERROR(clientId, new Exception("client " + clientId + " disconnected while serving fetch request")), null);
            return;
        }
        _channel.sendMessageWithAsyncReply(Message.FETCH_ENTRY(remoteClientId, key), server.getClientFetchTimeout(), new ReplyCallback() {

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

    void processIdleConnection() {
        Channel _channel = channel;
        if (_channel != null && _channel.isValid()) {
            _channel.channelIdle();
        }
    }

    public int getFetchPriority() {
        return fetchPriority;
    }

}
