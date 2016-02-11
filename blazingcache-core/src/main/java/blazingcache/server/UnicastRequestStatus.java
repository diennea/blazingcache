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

/**
 * Status of a unicast request
 *
 * @author enrico.olivelli
 */
public class UnicastRequestStatus {

    private final static AtomicLong newId = new AtomicLong(0);

    private final String sourceClientId;
    private final String destinationClientId;
    private final String description;
    private final long id;
    private BroadcastRequestStatusMonitor broadcastRequestStatusMonitor;

    public UnicastRequestStatus(String sourceClientId, String destinationClientId, String description) {
        this.sourceClientId = sourceClientId;
        this.destinationClientId = destinationClientId;
        this.description = description;
        this.id = newId.incrementAndGet();
    }

    public String getSourceClientId() {
        return sourceClientId;
    }

    public String getDestinationClientId() {
        return destinationClientId;
    }

    public String getDescription() {
        return description;
    }

    public long getId() {
        return id;
    }

    public BroadcastRequestStatusMonitor getBroadcastRequestStatusMonitor() {
        return broadcastRequestStatusMonitor;
    }

    public void setBroadcastRequestStatusMonitor(BroadcastRequestStatusMonitor broadcastRequestStatusMonitor) {
        this.broadcastRequestStatusMonitor = broadcastRequestStatusMonitor;
    }

}
