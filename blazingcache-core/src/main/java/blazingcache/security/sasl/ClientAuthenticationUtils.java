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
package blazingcache.security.sasl;

import blazingcache.network.Channel;
import blazingcache.network.Message;
import java.util.logging.Logger;

/**
 * Utility for client-side auth
 *
 * @author enrico.olivelli
 */
public class ClientAuthenticationUtils {

    private static final Logger LOGGER = Logger.getLogger(SaslNettyClient.class.getName());

    public static void performAuthentication(Channel _channel, String serverHostname, String sharedSecret) throws Exception {

        SaslNettyClient saslNettyClient = new SaslNettyClient(
            "client", sharedSecret,
            serverHostname
        );

        byte[] firstToken = new byte[0];
        if (saslNettyClient.hasInitialResponse()) {
            firstToken = saslNettyClient.evaluateChallenge(new byte[0]);
        }
        Message saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_REQUEST(SaslUtils.AUTH_DIGEST_MD5, firstToken), 10000);

        for (int i = 0; i < 100; i++) {
            byte[] responseToSendToServer;
            switch (saslResponse.type) {
                case Message.TYPE_SASL_TOKEN_SERVER_RESPONSE:
                    byte[] token = (byte[]) saslResponse.parameters.get("token");
                    responseToSendToServer = saslNettyClient.evaluateChallenge(token);
                    saslResponse = _channel.sendMessageWithReply(Message.SASL_TOKEN_MESSAGE_TOKEN(responseToSendToServer), 10000);
                    if (saslNettyClient.isComplete()) {
                        LOGGER.severe("SASL auth completed with success");
                        return;
                    }
                    break;
                case Message.TYPE_ERROR:
                    throw new Exception("Server returned ERROR during SASL negotiation, Maybe authentication failure (" + saslResponse.parameters + ")");
                default:
                    throw new Exception("Unexpected server response during SASL negotiation (" + saslResponse + ")");
            }
        }
        throw new Exception("SASL negotiation took too many steps");
    }
}
