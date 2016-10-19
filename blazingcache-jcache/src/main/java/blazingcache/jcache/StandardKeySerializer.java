/*
 * Copyright 2015 Diennea S.R.L..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package blazingcache.jcache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import javax.cache.CacheException;
import org.jboss.netty.util.internal.ConcurrentWeakKeyHashMap;

/**
 * Standard keys serializer
 *
 * @author enrico.olivelli
 */
public class StandardKeySerializer implements Serializer<Object, String> {

    private final ConcurrentWeakKeyHashMap<Object, String> notSerializableKeys = new ConcurrentWeakKeyHashMap<>();

    @Override
    public String serialize(Object value) {
        if (value instanceof String) {
            // BlazingCache is made for string keys!
            // if you are not using strings then implement a custom serializer
            String key = (String) value;
            if (!key.startsWith("$")) {
                return key;
            }
        }
        try {
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(oo);
            o.writeUnshared(value);
            o.flush();
            return "$" + Base64.getEncoder().encodeToString(oo.toByteArray());
        } catch (java.io.NotSerializableException notSerializable) {
            // VERY BAD CASE (the TCK for instance), Usually keys are serializable or the client uses a custom serializer
            String alreadyCached = notSerializableKeys.get(value);
            if (alreadyCached != null) {
                return alreadyCached;
            }
            String id = value.getClass().getName() + "_" + System.identityHashCode(value);
            notSerializableKeys.put(value, id);
            return id;
        } catch (IOException err) {
            throw new CacheException(err);
        }
    }

    @Override
    public Object deserialize(String keyOnCacheClient) {
        if (!keyOnCacheClient.startsWith("$")) {
            return keyOnCacheClient;
        }
        Object notSerializable = notSerializableKeys.get(keyOnCacheClient);
        if (notSerializable != null) {
            return notSerializable;
        }
        try {
            byte[] decoded = Base64.getDecoder().decode(keyOnCacheClient.substring(1));
            ByteArrayInputStream oo = new ByteArrayInputStream(decoded);
            ObjectInputStream o = new ObjectInputStream(oo);
            return o.readUnshared();
        } catch (IOException | ClassNotFoundException err) {
            throw new CacheException(err);
        }
    }

}
