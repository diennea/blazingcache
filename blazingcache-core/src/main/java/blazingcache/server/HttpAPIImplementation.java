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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Implementation of the HTTP API, both for embedded and for standalone installation
 *
 * @author enrico.olivelli
 */
public class HttpAPIImplementation {

    private static final Logger LOGGER = Logger.getLogger(HttpAPIImplementation.class.getName());

    public static void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        CacheServer broker = (CacheServer) JVMServersRegistry.getDefaultServer();
        String view = req.getParameter("view");
        if (view == null) {
            view = "status";
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("ok", "true");
        resultMap.put("version", CacheServer.VERSION());
        if (broker == null) {
            resultMap.put("ok", "false");
            resultMap.put("status", "not_started");
        } else {
            resultMap.put("status", broker.isLeader() ? "LEADER" : "BACKUP");
        }
        switch (view) {
            case "status": {
                if (broker != null) {
                    CacheStatus status = broker.getCacheStatus();
                    BroadcastRequestStatusMonitor monitor = broker.getNetworkRequestsStatusMonitor();
                    resultMap.put("entryCount", status.getTotalEntryCount());
                    resultMap.put("clientsWithData", status.getAllClientsWithListener());
                    resultMap.put("connections", broker.getAcceptor().getConnections().size());
                    {
                        List<BroadcastRequestStatus> broadcasts = monitor.getActualBroadcasts();
                        List<Map<String, Object>> bb = broadcasts.stream().map(b -> {
                            Map<String, Object> bm = new HashMap<>();
                            bm.put("description", b.getDescription());
                            bm.put("remainingClients", b.getRemaingClients());
                            return bm;
                        }).collect(Collectors.toList());
                        resultMap.put("broadcasts", bb);
                    }
                    {
                        List<UnicastRequestStatus> unicasts = monitor.getActualUnicasts();
                        List<Map<String, Object>> bb = unicasts.stream().map(b -> {
                            Map<String, Object> bm = new HashMap<>();
                            bm.put("description", b.getDescription());
                            bm.put("destinationClient", b.getDestinationClientId());
                            bm.put("sourceClient", b.getSourceClientId());
                            return bm;
                        }).collect(Collectors.toList());
                        resultMap.put("unicasts", bb);
                    }
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            }

            case "clients":
                if (broker != null) {
                    List<String> copy = new ArrayList<>(broker.getCacheStatus().getAllClientsWithListener());
                    copy.sort(String.CASE_INSENSITIVE_ORDER);
                    resultMap.put("clients", copy);
                } else {
                    resultMap.put("status", "not_started");
                    resultMap.put("clients", new ArrayList<String>());
                }
                break;
            case "keys":
                if (broker != null) {
                    List<String> copy = new ArrayList<>(broker.getCacheStatus().getKeys());
                    copy.sort(String.CASE_INSENSITIVE_ORDER);
                    resultMap.put("keys", copy);
                } else {
                    resultMap.put("status", "not_started");
                    resultMap.put("clients", new ArrayList<String>());
                }
                break;
            case "client":
                if (broker != null) {
                    String clientId = req.getParameter("client") + "";
                    CacheStatus status = broker.getCacheStatus();
                    resultMap.put("client", clientId);
                    resultMap.put("keys", status.getKeysForClient(clientId));
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            case "key":
                if (broker != null) {
                    String key = req.getParameter("key") + "";
                    CacheStatus status = broker.getCacheStatus();
                    resultMap.put("key", key);
                    resultMap.put("clients", status.getClientsForKey(key));
                } else {
                    resultMap.put("status", "not_started");
                }
                break;
            default:
                resultMap.put("error", "undefined view " + view);
                return;
        }

        ObjectMapper mapper = new ObjectMapper();
        LOGGER.log(Level.FINE, "GET  -> " + resultMap);
        String s = mapper.writeValueAsString(resultMap);
        byte[] res = s.getBytes(StandardCharsets.UTF_8);

        resp.setContentLength(res.length);

        resp.setContentType("application/json;charset=utf-8");
        resp.getOutputStream().write(res);
        resp.getOutputStream().close();
    }

}
