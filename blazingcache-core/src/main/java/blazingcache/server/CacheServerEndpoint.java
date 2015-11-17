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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import blazingcache.network.Channel;
import blazingcache.network.ServerSideConnectionAcceptor;

/**
 * Connections manager broker-side
 *
 * @author enrico.olivelli
 */
public class CacheServerEndpoint implements ServerSideConnectionAcceptor<CacheServerSideConnection> {

    private static final Logger LOGGER = Logger.getLogger(CacheServerEndpoint.class.getName());
    private final Map<String, CacheServerSideConnection> clientConnections = new ConcurrentHashMap<>();
    private final Map<Long, CacheServerSideConnection> connections = new ConcurrentHashMap<>();

    private final CacheServer broker;

    public CacheServerEndpoint(CacheServer broker) {
        this.broker = broker;
    }

    @Override
    public CacheServerSideConnection createConnection(Channel channel) {
        CacheServerSideConnection connection = new CacheServerSideConnection();
        connection.setBroker(broker);
        connection.setChannel(channel);
        channel.setMessagesReceiver(connection);
        connections.put(connection.getConnectionId(), connection);
        return connection;
    }

    public Map<String, CacheServerSideConnection> getClientConnections() {
        return clientConnections;
    }

    public Map<Long, CacheServerSideConnection> getConnections() {
        return connections;
    }

    CacheServerSideConnection getActualConnectionFromClient(String workerId) {
        return clientConnections.get(workerId);
    }

    void connectionAccepted(CacheServerSideConnection con) {
        LOGGER.log(Level.SEVERE, "connectionAccepted {0}", con);
        clientConnections.put(con.getClientId(), con);
    }

    void connectionClosed(CacheServerSideConnection con) {
        LOGGER.log(Level.SEVERE, "connectionClosed {0}", con);
        connections.remove(con.getConnectionId());
        if (con.getClientId() != null) {
            clientConnections.remove(con.getClientId()); // to be remove only if the connection is the current connection
        }
    }

}
