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

import javax.cache.processor.MutableEntry;

/**
 * Implementation of MutableEntry
 *
 * @author enrico.olivelli
 */
public class BlazingCacheCacheMutableEntry<K, V> extends BlazingCacheEntry<K, V> implements MutableEntry<K, V> {

    public BlazingCacheCacheMutableEntry(boolean present, K key, V value) {
        super(key, value);
        this.present = present;
    }

    private final boolean present;
    private boolean removed;
    private boolean updated;
    private boolean accessed;

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        } else {
            throw new IllegalArgumentException();
        }

    }

    public boolean isRemoved() {
        return removed;
    }

    public boolean isUpdated() {
        return updated;
    }

    public boolean isAccessed() {
        return accessed;
    }

    @Override
    public V getValue() {
        V actualValue = super.getValue();
        if (!updated && present) {
            // current value has been set by the EntryProcessor, so this is not an 'access'            
            accessed = true;
        }
        return actualValue;
    }

    @Override
    public void setValue(V value) {
        if (value == null) {
            throw new NullPointerException();
        }
        this.value = value;
        removed = false;
        updated = true;
    }

    @Override
    public boolean exists() {
        return value != null;
    }

    @Override
    public void remove() {
        value = null;
        if (!updated) {
            removed = true;
        }
        updated = false;
    }

}
