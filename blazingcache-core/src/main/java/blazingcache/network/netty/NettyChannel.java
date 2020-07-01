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
package blazingcache.network.netty;

import blazingcache.network.Channel;
import blazingcache.network.Message;
import blazingcache.network.ReplyCallback;
import blazingcache.network.SendResultCallback;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Channel implemented on Netty
 *
 * @author enrico.olivelli
 */
public class NettyChannel extends Channel {

    private static final boolean DISCONNECT_ON_PENDING_REPLY_TIMEOUT = Boolean.parseBoolean(System.getProperty("blazingcache.nettychannel.disconnectonpendingreplytimeout", "true"));
    private volatile SocketChannel socket;
    private static final Logger LOGGER = Logger.getLogger(NettyChannel.class.getName());
    private static final AtomicLong idGenerator = new AtomicLong();

    private final Map<String, ReplyCallback> pendingReplyMessages = new ConcurrentHashMap<>();
    private final Map<String, Message> pendingReplyMessagesSource = new ConcurrentHashMap<>();
    private final Map<String, Long> pendingReplyMessagesDeadline = new ConcurrentHashMap<>();
    private final ExecutorService callbackexecutor;
    private final NettyConnector connector;
    private volatile boolean ioErrors = false;
    private final long id = idGenerator.incrementAndGet();
    private final boolean disconnectOnReplyTimeout;

    @Override
    public String toString() {
        return "NettyChannel{name=" + name + ", id=" + id + ", socket=" + socket + " pending " + pendingReplyMessages.size() + " msgs}";
    }

    public NettyChannel(String name, SocketChannel socket, ExecutorService callbackexecutor, NettyConnector connector) {
        this.name = name;
        this.socket = socket;
        this.callbackexecutor = callbackexecutor;
        this.connector = connector;
        this.disconnectOnReplyTimeout = (connector == null); // only server-side
    }

    public void messageReceived(Message message) {
        if (message.getReplyMessageId() != null) {
            handleReply(message);
        } else {
            submitCallback(() -> {
                try {
                    messagesReceiver.messageReceived(message);
                } catch (Throwable t) {
                    LOGGER.log(Level.SEVERE, this + ": error " + t, t);
                    close();
                }
            });
        }
    }

    private void handleReply(Message anwermessage) {
        final ReplyCallback callback = pendingReplyMessages.get(anwermessage.getReplyMessageId());
        pendingReplyMessages.remove(anwermessage.getReplyMessageId());
        pendingReplyMessagesDeadline.remove(anwermessage.getReplyMessageId());
        Message original = pendingReplyMessagesSource.remove(anwermessage.getReplyMessageId());
        if (callback != null && original != null) {
            submitCallback(() -> {
                callback.replyReceived(original, anwermessage, null);
            });
        }
    }

    @Override
    public void sendOneWayMessage(Message message, SendResultCallback callback) {
        if (message.getMessageId() == null) {
            message.setMessageId(UUID.randomUUID().toString());
        }
        SocketChannel _socket = this.socket;
        if (_socket == null || !_socket.isOpen()) {
            callback.messageSent(message, new Exception(this + " connection is closed"));
            return;
        }
        _socket.writeAndFlush(message).addListener(new GenericFutureListener() {

            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    callback.messageSent(message, null);
                } else {
                    LOGGER.log(Level.SEVERE, this + ": error " + future.cause(), future.cause());
                    callback.messageSent(message, future.cause());
                    close();
                }
            }

        });
    }

    @Override
    public void sendReplyMessage(Message inAnswerTo, Message message) {
        if (message.getMessageId() == null) {
            message.setMessageId(UUID.randomUUID().toString());
        }
        if (this.socket == null) {
            LOGGER.log(Level.SEVERE, this + " channel not active, discarding reply message " + message);
            return;
        }
        message.setReplyMessageId(inAnswerTo.messageId);
        sendOneWayMessage(message, new SendResultCallback() {

            @Override
            public void messageSent(Message originalMessage, Throwable error) {
                if (error != null) {
                    LOGGER.log(Level.SEVERE, this + " error:" + error, error);
                }
            }
        });
    }

    private void processPendingReplyMessagesDeadline() {
        List<String> messagesWithNoReply = new ArrayList<>();
        long now = System.currentTimeMillis();
        pendingReplyMessagesDeadline.forEach((messageId, deadline) -> {
            if (deadline < now) {
                messagesWithNoReply.add(messageId);
            }
        });
        if (messagesWithNoReply.isEmpty()) {
            return;
        }
        if (DISCONNECT_ON_PENDING_REPLY_TIMEOUT && disconnectOnReplyTimeout) {
            LOGGER.log(Level.SEVERE, this + " found " + messagesWithNoReply.size() + " without reply, channel will be closed");
            ioErrors = true;
        } else {
            LOGGER.log(Level.SEVERE, this + " found " + messagesWithNoReply.size() + " without reply");
        }
        for (String messageId : messagesWithNoReply) {
            Message original = pendingReplyMessagesSource.remove(messageId);
            ReplyCallback callback = pendingReplyMessages.remove(messageId);
            pendingReplyMessagesDeadline.remove(messageId);
            if (original != null && callback != null) {
                submitCallback(() -> {
                    callback.replyReceived(original, null, new IOException(this + " reply timeout expired"));
                });
            }
        }
        if (DISCONNECT_ON_PENDING_REPLY_TIMEOUT && disconnectOnReplyTimeout) {
            close();
        }
    }

    @Override
    public void sendMessageWithAsyncReply(Message message, long timeout, ReplyCallback callback) {
        if (message.getMessageId() == null) {
            message.setMessageId(UUID.randomUUID().toString());
        }
        if (!isValid()) {
            submitCallback(() -> {
                callback.replyReceived(message, null, new Exception(this + " connection is not active"));
            });
            return;
        }
        pendingReplyMessages.put(message.getMessageId(), callback);
        pendingReplyMessagesSource.put(message.getMessageId(), message);
        pendingReplyMessagesDeadline.put(message.getMessageId(), System.currentTimeMillis() + timeout);
        sendOneWayMessage(message, new SendResultCallback() {

            @Override
            public void messageSent(Message originalMessage, Throwable error) {
                if (error != null) {
                    LOGGER.log(Level.SEVERE, this + ": error while sending reply message to " + originalMessage, error);
                    submitCallback(() -> {
                        callback.replyReceived(message, null, new Exception(this + ": error while sending reply message to " + originalMessage, error));
                    });
                }
            }
        });
    }

    @Override
    public boolean isValid() {
        SocketChannel _socket = socket;
        return _socket != null && _socket.isOpen() && !ioErrors;
    }

    private volatile boolean closed = false;

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOGGER.log(Level.SEVERE, this + ": closing");
        String socketDescription = socket + "";
        if (socket != null) {
            try {
                socket.close().await();
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
            } finally {
                socket = null;
            }
        }

        pendingReplyMessages.forEach((key, callback) -> {
            Message original = pendingReplyMessagesSource.remove(key);
            LOGGER.log(Level.SEVERE, this + " message " + key + " was not replied (" + original + ") callback:" + callback);
            if (original != null) {
                submitCallback(() -> {
                    callback.replyReceived(original, null, new IOException("comunication channel is closed. Cannot wait for pending messages, socket=" + socketDescription));
                });
            }
        });
        pendingReplyMessages.clear();
        pendingReplyMessagesSource.clear();
        pendingReplyMessagesDeadline.clear();

        if (connector != null) {
            connector.close();
        }
    }

    public void exceptionCaught(Throwable cause) {
        LOGGER.log(Level.SEVERE, this + " io-error " + cause, cause);
        ioErrors = true;
    }

    void channelClosed() {
        submitCallback(() -> {
            if (this.messagesReceiver != null) {
                this.messagesReceiver.channelClosed();
            }
        });
    }

    private void submitCallback(Runnable runnable) {
        try {
            callbackexecutor.submit(runnable);
        } catch (RejectedExecutionException stopped) {
            LOGGER.log(Level.SEVERE, this + " rejected runnable " + runnable, stopped);
        }
    }

    @Override
    public void channelIdle() {
        LOGGER.log(Level.FINEST, "{0} channelIdle", this);
        processPendingReplyMessagesDeadline();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

}
