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

import blazingcache.utils.RawString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coalesces concurrent invalidations of the same key (issue #188).
 * <p>
 * Invalidating a key is idempotent: while an invalidation of a key is in flight
 * (holding the per-key write lock and broadcasting to the interested clients), any
 * further invalidation request for the same key can attach to the in-flight one
 * instead of queueing behind the lock for another full network round-trip. When the
 * in-flight invalidation completes, every attached requester is notified with the
 * same result.
 * <p>
 * This is safe because the in-flight invalidation holds the per-key write lock for
 * the whole broadcast, which is mutually exclusive with fetches (read lock): no
 * client can register itself for the key between the moment the interested-clients
 * set is snapshotted and the moment the broadcast completes. Therefore any request
 * that arrives while the broadcast is in flight is fully satisfied by it.
 *
 * @author diego.salvi
 */
class PendingInvalidationsManager {

    /**
     * A group of invalidation requests for the same key that share a single
     * broadcast.
     */
    static final class PendingInvalidation {

        private final List<SimpleCallback<RawString>> callbacks =
                Collections.synchronizedList(new ArrayList<>());

        private void attach(SimpleCallback<RawString> callback) {
            callbacks.add(callback);
        }

        List<SimpleCallback<RawString>> getCallbacks() {
            return callbacks;
        }
    }

    private final ConcurrentHashMap<RawString, PendingInvalidation> pending = new ConcurrentHashMap<>();

    /**
     * Registers an invalidation request for a key.
     *
     * @return {@code true} if the caller is the owner of a NEW in-flight
     * invalidation and must therefore perform the actual broadcast; {@code false}
     * if the request has been coalesced into an already in-flight invalidation and
     * the caller must not do anything (its callback will be invoked when the
     * in-flight invalidation completes).
     */
    boolean register(RawString key, SimpleCallback<RawString> onFinish) {
        final boolean[] owner = {false};
        pending.compute(key, (k, existing) -> {
            if (existing == null) {
                existing = new PendingInvalidation();
                owner[0] = true;
            }
            existing.attach(onFinish);
            return existing;
        });
        return owner[0];
    }

    /**
     * Marks the in-flight invalidation of a key as completed and returns every
     * callback that must be notified (the owner's plus all the coalesced ones).
     * Any invalidation request arriving after this call will start a fresh
     * broadcast.
     */
    List<SimpleCallback<RawString>> complete(RawString key) {
        PendingInvalidation removed = pending.remove(key);
        if (removed == null) {
            return Collections.emptyList();
        }
        return removed.getCallbacks();
    }

    int getNumberOfPendingInvalidations() {
        return pending.size();
    }
}
