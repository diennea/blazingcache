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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Stato invalidazione
 *
 * @author enrico.olivelli
 */
public final class BroadcastRequestStatus {

    private final static AtomicLong newId = new AtomicLong(0);
    private final long id;
    private final String description;
    private final Set<String> remaingClients;
    private final SimpleCallback<String> onFinish;
    private final SimpleCallback<String> onClientDone;
    private final ReentrantLock lock = new ReentrantLock(false);
    private boolean done;

    public BroadcastRequestStatus(String description, Set<String> remaingClients, SimpleCallback<String> onFinish, SimpleCallback<String> onClientDone) {
        this.description = description;
        this.id = newId.incrementAndGet();
        this.remaingClients = new HashSet<>(remaingClients);
        this.onFinish = onFinish;
        this.onClientDone = onClientDone;
    }

    public String getDescription() {
        return description;
    }

    public Set<String> getRemaingClients() {
        lock.lock();
        try {
            return new HashSet<>(remaingClients);
        } finally {
            lock.unlock();
        }
    }

    public long getId() {
        return id;
    }

    public void clientDone(String clientId) {
        if (onClientDone != null) {
            onClientDone.onResult(clientId, null);
        }
        boolean i_did_it = false;
        lock.lock();
        try {
            if (done) {
                return;
            }
            remaingClients.remove(clientId);
            if (remaingClients.isEmpty()) {
                done = true;
                i_did_it = true;
            }
        } finally {
            lock.unlock();
        }
        // solo un thread arriva qui
        if (i_did_it) {
            try {
                onFinish.onResult(null, null);
            } finally {
                BroadcastRequestStatusMonitor.unregister(this);
            }

        }
    }

}
