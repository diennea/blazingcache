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

import blazingcache.utils.RawString;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author francesco.caliumi
 */
public class PendingFetchesManagerTest {
    @Test
    public void test() throws Exception {
        PendingFetchesManager man = new PendingFetchesManager();
        
        for (int i=0; i<100; i++) {
            RawString key = new RawString(("fetch"+i).getBytes());
            long fetch1 = man.registerFetchForKey(key);
            long fetch2 = man.registerFetchForKey(key);
            long fetch3 = man.registerFetchForKey(key);
            man.consumeAndValidateFetchForKey(key, fetch1);
            man.consumeAndValidateFetchForKey(key, fetch2);
            man.consumeAndValidateFetchForKey(key, fetch3);
        }
        
        System.out.println(">>> "+man.getPendingFetchesForTest());
        assertEquals(0, man.getPendingFetchesForTest().size());
    }
}
