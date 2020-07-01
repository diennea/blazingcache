/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package blazingcache.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;


public class CountdownWatcher implements Watcher {
    
    protected static final Logger LOG = Logger.getLogger("" + CountdownWatcher.class);
    private final String name;
    private CountDownLatch clientConnected;
    private Event.KeeperState state;
    private boolean connected;
    private boolean expired;

    public CountdownWatcher(String name) {
        this.name = name;
        reset();
    }

    private synchronized void reset() {
        clientConnected = new CountDownLatch(1);
        state = Event.KeeperState.Disconnected;
        connected = false;
        expired = false;
    }

    public void process(WatchedEvent event) {
        LOG.info("Watcher " + name + " got event " + event);
        setState(event.getState());
        setConnected(true);
        clientConnected.countDown();
        if (event.getState() == Event.KeeperState.Expired) {
            expired = true;
        }
    }

    public synchronized boolean isConnected() {
        return connected;
    }

    public synchronized boolean isExpired() {
        return expired;
    }

    public synchronized Event.KeeperState state() {
        return state;
    }

    public void setState(Event.KeeperState state) {
        this.state = state;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public void setExpired(boolean expired) {
        this.expired = expired;
    }

    public void waitForConnected(long timeout) throws InterruptedException, TimeoutException {
        Assert.assertTrue(clientConnected.await(timeout, TimeUnit.MILLISECONDS));
    }

    public void waitForExpired(long timeout) throws InterruptedException, TimeoutException {
        long expire = System.currentTimeMillis() + timeout;
        long left = timeout;
        while (!isExpired() && left > 0) {
            Thread.sleep(left);
            left = expire - System.currentTimeMillis();
        }
        if (!isConnected()) {
            throw new TimeoutException("Did not disconnect");
        }
    }
    
}
