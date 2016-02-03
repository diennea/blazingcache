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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Runnable wrapper which changes the current Thread name and handles uncaught
 * exceptions
 *
 * @author enrico.olivelli
 */
public final class ManagedRunnable implements Runnable {

    private final String threadName;
    private final Runnable wrapped;
    private final static Logger LOGGER = Logger.getLogger(ManagedRunnable.class.getName());

    public ManagedRunnable(String threadName, Runnable wrapped) {
        this.threadName = threadName;
        this.wrapped = wrapped;
    }

    @Override
    public void run() {
        final String oldName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(threadName);
            wrapped.run();
        } catch (Throwable error) {
            LOGGER.log(Level.SEVERE, "Unhandled error at operation " + threadName + ":" + error, error);
        } finally {
            Thread.currentThread().setName(oldName);
        }
    }

}
