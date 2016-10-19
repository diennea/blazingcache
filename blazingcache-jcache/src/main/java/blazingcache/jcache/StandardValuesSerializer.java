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

import blazingcache.client.impl.JDKEntrySerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.cache.CacheException;

/**
 * Standard values serializer
 *
 * @author enrico.olivelli
 */
public class StandardValuesSerializer implements Serializer<Object, byte[]> {

    private final static JDKEntrySerializer STANDARD = new JDKEntrySerializer();

    @Override
    public byte[] serialize(Object value) {
        try {
            return STANDARD.serializeObject(null, value);
        } catch (blazingcache.client.CacheException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Object deserialize(byte[] cachedValue) {
        try {
            return STANDARD.deserializeObject(null, cachedValue);
        } catch (blazingcache.client.CacheException ex) {
            throw new RuntimeException(ex);
        }
    }

}
