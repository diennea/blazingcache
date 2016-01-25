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

/**
 * ID of a named lock
 *
 * @author enrico.olivelli
 */
public class LockID {

    public final long stamp;

    public static final LockID VALIDATED_CLIENT_PROVIDED_LOCK = new LockID(Long.MAX_VALUE);

    public LockID(long stamp) {
        this.stamp = stamp;
    }

    @Override
    public String toString() {
        return "LockID{" + "stamp=" + stamp + '}';
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 13 * hash + (int) (this.stamp ^ (this.stamp >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final LockID other = (LockID) obj;
        if (this.stamp != other.stamp) {
            return false;
        }
        return true;
    }

}
