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
package blazingcache.client.impl;

import blazingcache.network.Channel;
import blazingcache.network.Message;

/**
 * Internal client listener, used for internals. This is not a public API
 *
 * @author enrico.olivelli
 */
public interface InternalClientListener {

    public default void onConnection(Channel channel) {
    }

    public default boolean messageReceived(Message message, Channel channel) {
        return true;
    }

    public default void onFetchResponse(String key, Message message) {
    }

    public default void onRequestSent(Message message) {
    }

}
