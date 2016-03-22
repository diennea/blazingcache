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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handle locks by key
 *
 * @author enrico.olivelli
 */
public class LocalLockManager {

    private static final Logger LOGGER = Logger.getLogger(LocalLockManager.class.getName());
    private static final boolean ENABLE_LOCAL_LOCKS = Boolean.parseBoolean(System.getProperty("blazingcache.client.enablelocallocks", "true"));

    private ReentrantLock makeLock() {
        return new ReentrantLock();
    }
    private final ReentrantLock generalLock = new ReentrantLock(true);
    private final Map<String, ReentrantLock> liveLocks = new HashMap<>();
    private final Map<String, AtomicInteger> locksCounter = new HashMap<>();

    private ReentrantLock makeLockForKey(String key) {
        ReentrantLock lock;
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

    private ReentrantLock returnLockForKey(String key) throws IllegalStateException {
        ReentrantLock lock;
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

    public ReentrantLock acquireWriteLockForKey(String key) {
        if (!ENABLE_LOCAL_LOCKS) {
            return null;
        }
        ReentrantLock lock = makeLockForKey(key);
        lock.lock();
        return lock;
    }

    public void releaseWriteLockForKey(String key, ReentrantLock lockStamp) {
        if (!ENABLE_LOCAL_LOCKS) {
            return;
        }
        ReentrantLock lock = returnLockForKey(key);
        lock.unlock();
    }

}
