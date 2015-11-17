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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Channel implemented on Netty
 *
 * @author enrico.olivelli
 */
public class NettyChannel extends Channel {

    volatile SocketChannel socket;
    private static final Logger LOGGER = Logger.getLogger(NettyChannel.class.getName());

    private final Map<String, ReplyCallback> pendingReplyMessages = new ConcurrentHashMap<>();
    private final Map<String, Message> pendingReplyMessagesSource = new ConcurrentHashMap<>();
    private final ExecutorService callbackexecutor;

    @Override
    public String toString() {
        return "NettyChannel{" + "socket=" + socket + '}';
    }

    public NettyChannel(SocketChannel socket, ExecutorService callbackexecutor) {
        this.socket = socket;
        this.callbackexecutor = callbackexecutor;
    }

    public void messageReceived(Message message) {
        if (message.getReplyMessageId() != null) {
            handleReply(message);
        } else {
            try {
                messagesReceiver.messageReceived(message);
            } catch (Throwable t) {
                LOGGER.log(Level.SEVERE, "error", t);
                close();
            }
        }
    }

    private void handleReply(Message anwermessage) {
//        LOGGER.severe("handleReply for " + anwermessage.getReplyMessageId());
        final ReplyCallback callback = pendingReplyMessages.get(anwermessage.getReplyMessageId());
        if (callback != null) {
            pendingReplyMessages.remove(anwermessage.getReplyMessageId());
            Message original = pendingReplyMessagesSource.remove(anwermessage.getReplyMessageId());
            if (original != null) {
                submitCallback(() -> {
                    callback.replyReceived(original, anwermessage, null);
                });
            }
        }
    }

    @Override
    public void sendOneWayMessage(Message message, SendResultCallback callback) {
        if (message.getMessageId() == null) {
            message.setMessageId(UUID.randomUUID().toString());
        }
        if (this.socket == null) {
            callback.messageSent(message, new Exception("connection is closed"));
            return;
        }
        this.socket.writeAndFlush(message).addListener(new GenericFutureListener() {

            @Override
            public void operationComplete(Future future) throws Exception {
                if (future.isSuccess()) {
                    callback.messageSent(message, null);
                } else {
                    LOGGER.log(Level.SEVERE, "error", future.cause());
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
            LOGGER.log(Level.SEVERE, "channel not active, discarding reply message " + message);
            return;
        }
        message.setReplyMessageId(inAnswerTo.messageId);
        sendOneWayMessage(message, new SendResultCallback() {

            @Override
            public void messageSent(Message originalMessage, Throwable error) {
                if (error != null) {
                    LOGGER.log(Level.SEVERE, "error", error);
                }
            }
        });
    }

    @Override
    public void sendMessageWithAsyncReply(Message message, ReplyCallback callback) {
        if (message.getMessageId() == null) {
            message.setMessageId(UUID.randomUUID().toString());
        }
        if (this.socket == null) {
            submitCallback(() -> {
                callback.replyReceived(message, null, new Exception("connection is not active"));
            });
            return;
        }
        pendingReplyMessages.put(message.getMessageId(), callback);
        pendingReplyMessagesSource.put(message.getMessageId(), message);
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
        return socket != null && socket.isOpen();
    }

    @Override
    public void close() {
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
            submitCallback(() -> {
                Message original = pendingReplyMessagesSource.remove(key);
                if (original != null) {
                    callback.replyReceived(original, null, new IOException("comunication channel is closed. Cannot wait for pending messages, socket=" + socketDescription));
                }
            });
        });
        pendingReplyMessages.clear();
    }

    void exceptionCaught(Throwable cause) {
        LOGGER.log(Level.SEVERE, this + " io-error", cause);
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

}
