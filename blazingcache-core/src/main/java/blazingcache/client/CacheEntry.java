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
package blazingcache.client;

import blazingcache.utils.RawString;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Una entry nella cache
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public final class CacheEntry implements AutoCloseable {

    private long lastGetTime;
    private final long putTime;
    private final RawString key;
    private final ByteBuf buf;
    private final long expiretime;
    private final int dataLength;
    private SoftReference<Object> reference;

    /**
     * Creates the entry and refcount of the given ByteBuf is not incremented
     *
     * @param key
     * @param lastGetTimeNanos
     * @param serializedData
     * @param expiretime
     * @param deserialized
     */
    CacheEntry(RawString key, long lastGetTimeNanos, ByteBuf serializedData, long expiretime, Object deserialized) {
        this.key = key;
        this.lastGetTime = lastGetTimeNanos;
        this.putTime = lastGetTimeNanos;
        this.buf = serializedData;
        this.expiretime = expiretime;
        this.dataLength = serializedData.readableBytes();
        this.reference = deserialized != null ? new SoftReference<>(deserialized) : null;
    }

    /**
     * Increase refcount
     */
    public void retain() {
        this.buf.retain();
    }

    /**
     * Releases the internal buffer
     */
    @Override
    public void close() {
        try {
            this.buf.release();
        } catch (RuntimeException err) {
            LOG.log(Level.SEVERE, "Error while releasing entry", err);
        }
    }

    synchronized Object resolveReference(EntrySerializer serializer) throws CacheException {
        Object resolved = reference != null ? reference.get() : null;
        if (resolved == null) {
            try (InputStream in = getSerializedDataStream()) {
                resolved = serializer.deserializeObject(key.toString(), in);
            } catch (IOException err) {
                throw new CacheException(err);
            }
            reference = new SoftReference<>(resolved);
        }
        return resolved;
    }

    public int getSerializedDataLength() {
        return dataLength;
    }

    public RawString getKey() {
        return key;
    }

    public long getPutTime() {
        return putTime;
    }

    public long getLastGetTime() {
        return lastGetTime;
    }

    public void setLastGetTime(final long lastGetTimeNanos) {
        this.lastGetTime = lastGetTimeNanos;
    }

    /**
     * Access to data. You have to close the stream in order to handle correctly
     * refcounts
     *
     * @return
     */
    public InputStream getSerializedDataStream() {
        return new ByteBufInputStream(buf.retainedSlice(),
                buf.readableBytes(), true /* releaseOnClose */);
    }

    public boolean isSerializedDataEqualTo(byte[] other) {
        // current thread must already have retained the entry
        // let Netty do the best not to copy memory
        return buf.equals(Unpooled.wrappedBuffer(other));
    }

    public byte[] getSerializedData() {
        // copy data from Direct Memory to Heap
        return ByteBufUtil.getBytes(buf);
    }
    private static final Logger LOG = Logger.getLogger(CacheEntry.class.getName());

    public long getExpiretime() {
        return expiretime;
    }

    @Override
    public String toString() {
        return "CacheEntry{" + "key=" + key + ", lastGetTime=" + lastGetTime + ", expiretime=" + expiretime + '}';
    }

    // only for tests
    synchronized void discardInternalCachedObject() {
        reference = null;
    }

}
