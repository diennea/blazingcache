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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle locks by key
 *
 * @author enrico.olivelli
 */
public class KeyedLockManager {

    private static final Logger LOGGER = Logger.getLogger(KeyedLockManager.class.getName());

    private StampedLock makeLock() {
        return new StampedLock();
    }
    private final ReentrantLock generalLock = new ReentrantLock(true);
    private final Map<RawString, StampedLock> liveLocks = new HashMap<>();
    private final Map<RawString, AtomicInteger> locksCounter = new HashMap<>();

    /**
     * Debug operation to see actual locked keys
     *
     * @return
     */
    public Map<RawString, Integer> getLockedKeys() {
        HashMap<RawString, Integer> result = new HashMap<>();
        generalLock.lock();
        try {
            locksCounter.forEach((k, v) -> {
                result.put(k, v.get());
            });
        } finally {
            generalLock.unlock();
        }
        return result;
    }

    /**
     *
     * @return the number of currently locked keys
     */
    public int getNumberOfLockedKeys() {
        this.generalLock.lock();
        try {
            return this.locksCounter.size();
        } finally {
            this.generalLock.unlock();
        }
    }

    private StampedLock makeLockForKey(RawString key) {
        StampedLock lock;
        generalLock.lock();
        try {
            lock = liveLocks.get(key);
            if (lock == null) {
                lock = makeLock();
                liveLocks.put(key, lock);
                locksCounter.put(key, new AtomicInteger(1));
            } else {
                locksCounter.get(key).incrementAndGet();
            }
        } finally {
            generalLock.unlock();
        }
        return lock;
    }

    private StampedLock getLockForKey(RawString key) {
        StampedLock lock;
        generalLock.lock();
        try {
            lock = liveLocks.get(key);
        } finally {
            generalLock.unlock();
        }
        return lock;
    }

    private StampedLock returnLockForKey(RawString key) throws IllegalStateException {
        StampedLock lock;
        generalLock.lock();
        try {
            lock = liveLocks.get(key);
            if (lock == null) {
                LOGGER.log(Level.SEVERE, "no lock object exists for key {0}", key);
                throw new IllegalStateException("no lock object exists for key " + key);
            }
            int actualCount = locksCounter.get(key).decrementAndGet();
            if (actualCount == 0) {
                liveLocks.remove(key);
                locksCounter.remove(key);
            }
        } finally {
            generalLock.unlock();
        }
        return lock;
    }

    LockID acquireWriteLockForKey(RawString key, String clientId, String clientProvidedLockId) {
        if (clientProvidedLockId != null) {
            return useClientProvidedLockForKey(key, Long.parseLong(clientProvidedLockId));
        } else {
            return acquireWriteLockForKey(key, clientId);
        }
    }

    LockID acquireWriteLockForKey(RawString key, String clientId) {
        StampedLock lock = makeLockForKey(key);
        LockID result = new LockID(lock.writeLock());
        return result;
    }

    void releaseWriteLockForKey(RawString key, String clientId, LockID lockStamp) {
        if (lockStamp == LockID.VALIDATED_CLIENT_PROVIDED_LOCK) {
            return;
        }
        StampedLock lock = returnLockForKey(key);
        lock.unlock(lockStamp.stamp);
    }

    LockID useClientProvidedLockForKey(RawString key, long stamp) {
        StampedLock lock = getLockForKey(key);
        if (lock.validate(stamp)) {
            return LockID.VALIDATED_CLIENT_PROVIDED_LOCK;
        } else {
            return null;
        }
    }

}
