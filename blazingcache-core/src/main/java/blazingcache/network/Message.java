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
package blazingcache.network;

import blazingcache.utils.RawString;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A message (from broker to worker or from worker to broker)
 *
 * @author enrico.olivelli
 */
public final class Message {

    public static Message ACK(String clientId) {
        return new Message(clientId, TYPE_ACK, new HashMap<>());
    }

    public static Message FETCH_ENTRY(String clientId, RawString key) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        return new Message(clientId, TYPE_FETCH_ENTRY, data);
    }

    public static Message TOUCH_ENTRY(String clientId, RawString key, long expiretime) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        data.put("expiretime", expiretime);
        return new Message(clientId, TYPE_TOUCH_ENTRY, data);
    }

    public static Message PUT_ENTRY(String clientId, RawString key, byte[] serializedData, long expiretime) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        data.put("data", serializedData);
        data.put("expiretime", expiretime);
        return new Message(clientId, TYPE_PUT_ENTRY, data);
    }

    public static Message LOAD_ENTRY(String clientId, RawString key, byte[] serializedData, long expiretime) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        data.put("data", serializedData);
        data.put("expiretime", expiretime);
        return new Message(clientId, TYPE_LOAD_ENTRY, data);
    }

    public static Message UNREGISTER_ENTRY(String clientId, List<RawString> keys) {
        HashMap<String, Object> data = new HashMap<>();
        // clients before 12.0 will set only one 'key'
        data.put("keys", keys);
        return new Message(clientId, TYPE_UNREGISTER_ENTRY, data);
    }

    public static Message ERROR(String clientId, Throwable error) {
        Map<String, Object> params = new HashMap<>();
        params.put("error", error + "");
        StringWriter writer = new StringWriter();
        error.printStackTrace(new PrintWriter(writer));
        params.put("stackTrace", writer.toString());
        return new Message(clientId, TYPE_ERROR, params);
    }

    public static Message SASL_TOKEN_MESSAGE_REQUEST(String saslMech, byte[] firstToken) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("mech", saslMech);
        data.put("token", firstToken);
        return new Message(null, TYPE_SASL_TOKEN_MESSAGE_REQUEST, data);
    }

    public static Message SASL_TOKEN_SERVER_RESPONSE(byte[] saslTokenChallenge) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("token", saslTokenChallenge);
        return new Message(null, TYPE_SASL_TOKEN_SERVER_RESPONSE, data);
    }

    public static Message SASL_TOKEN_MESSAGE_TOKEN(byte[] token) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("token", token);
        return new Message(null, TYPE_SASL_TOKEN_MESSAGE_TOKEN, data);
    }

    public static Message CLIENT_CONNECTION_REQUEST(String clientId, String secret, int fetchPriority) {
        HashMap<String, Object> data = new HashMap<>();
        String ts = System.currentTimeMillis() + "";
        data.put("ts", ts);
        data.put("challenge", HashUtils.sha1(ts + "#" + secret));
        data.put("fetchPriority", fetchPriority);
        return new Message(clientId, TYPE_CLIENT_CONNECTION_REQUEST, data);
    }

    public static Message CLIENT_SHUTDOWN(String clientId) {
        return new Message(clientId, TYPE_CLIENT_SHUTDOWN, new HashMap<>());
    }

    public static Message INVALIDATE(String clientId, RawString key) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        return new Message(clientId, TYPE_INVALIDATE, data);
    }

    public static Message LOCK(String clientId, RawString key) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        return new Message(clientId, TYPE_LOCK_ENTRY, data);
    }

    public static Message UNLOCK(String clientId, RawString key, String lockId) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("key", key);
        data.put("lockId", lockId);
        return new Message(clientId, TYPE_UNLOCK_ENTRY, data);
    }

    public static Message INVALIDATE_BY_PREFIX(String clientId, RawString prefix) {
        HashMap<String, Object> data = new HashMap<>();
        data.put("prefix", prefix);
        return new Message(clientId, TYPE_INVALIDATE_BY_PREFIX, data);
    }

    public final String clientId;
    public final int type;
    public final Map<String, Object> parameters;
    public String messageId;
    public String replyMessageId;

    @Override
    public String toString() {
        return typeToString(type) + ", " + parameters;

    }

    public static final int TYPE_ACK = 1;
    public static final int TYPE_CLIENT_CONNECTION_REQUEST = 2;
    public static final int TYPE_CLIENT_SHUTDOWN = 3;
    public static final int TYPE_INVALIDATE = 4;
    public static final int TYPE_ERROR = 5;
    public static final int TYPE_PUT_ENTRY = 6;
    public static final int TYPE_INVALIDATE_BY_PREFIX = 7;
    public static final int TYPE_UNREGISTER_ENTRY = 8;
    public static final int TYPE_FETCH_ENTRY = 9;
    public static final int TYPE_TOUCH_ENTRY = 10;
    public static final int TYPE_LOCK_ENTRY = 11;
    public static final int TYPE_UNLOCK_ENTRY = 12;
    public static final int TYPE_LOAD_ENTRY = 13;

    public static final int TYPE_SASL_TOKEN_MESSAGE_REQUEST = 100;
    public static final int TYPE_SASL_TOKEN_SERVER_RESPONSE = 101;
    public static final int TYPE_SASL_TOKEN_MESSAGE_TOKEN = 102;

    public static String typeToString(int type) {
        switch (type) {
            case TYPE_ACK:
                return "ACK";
            case TYPE_ERROR:
                return "ERROR";
            case TYPE_CLIENT_CONNECTION_REQUEST:
                return "CLIENT_CONNECTION_REQUEST";
            case TYPE_CLIENT_SHUTDOWN:
                return "CLIENT_SHUTDOWN";
            case TYPE_INVALIDATE:
                return "INVALIDATE";
            case TYPE_INVALIDATE_BY_PREFIX:
                return "INVALIDATE_BY_PREFIX";
            case TYPE_PUT_ENTRY:
                return "PUT_ENTRY";
            case TYPE_UNREGISTER_ENTRY:
                return "UNREGISTER_ENTRY";
            case TYPE_FETCH_ENTRY:
                return "FETCH_ENTRY";
            case TYPE_TOUCH_ENTRY:
                return "TOUCH_ENTRY";
            case TYPE_LOCK_ENTRY:
                return "LOCK_ENTRY";
            case TYPE_UNLOCK_ENTRY:
                return "UNLOCK_ENTRY";
            case TYPE_LOAD_ENTRY:
                return "LOAD_ENTRY";
            case TYPE_SASL_TOKEN_MESSAGE_REQUEST:
                return "SASL_TOKEN_MESSAGE_REQUEST";
            case TYPE_SASL_TOKEN_SERVER_RESPONSE:
                return "SASL_TOKEN_SERVER_RESPONSE";
            case TYPE_SASL_TOKEN_MESSAGE_TOKEN:
                return "SASL_TOKEN_MESSAGE_TOKEN";
            default:
                return "?" + type;
        }
    }

    public Message(String workerProcessId, int type, Map<String, Object> parameters) {
        this.clientId = workerProcessId;
        this.type = type;
        this.parameters = parameters;
    }

    public String getMessageId() {
        return messageId;
    }

    public Message setMessageId(String messageId) {
        this.messageId = messageId;
        return this;
    }

    public String getReplyMessageId() {
        return replyMessageId;
    }

    public Message setReplyMessageId(String replyMessageId) {
        this.replyMessageId = replyMessageId;
        return this;
    }

    public Message setParameter(String key, Object value) {
        this.parameters.put(key, value);
        return this;
    }
}
