/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.envelope;

import io.netty.buffer.ByteBuf;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;

/** An {@link Decoder} for binary-format data.
 * <p/>
 * Instances are created using {@link DecoderFactory}.
 * <p/>
 * This class may read-ahead and buffer bytes from the source beyond what is
 * required to serve its read methods.
 * The number of unused bytes in the buffer can be accessed by
 * inputStream().remaining(), if the BinaryDecoder is not 'direct'.
 * 
 * @see ByteBufEncoder
 */

public class ByteBufDecoder extends Decoder {
  // we keep the buffer and its state variables in this class and not in a
  // container class for performance reasons. This improves performance
  // over a container object by about 5% to 15%
  // for example, we could have a FastBuffer class with these state variables
  // and keep a private FastBuffer member here. This simplifies the
  // "detach source" code and source access to the buffer, but
  // hurts performance.
  private ByteBuf buf = null;

  public ByteBufDecoder(ByteBuf buf) {
    this.buf = buf;
  }

  @Override
  public void readNull() throws IOException {
  }

  @Override
  public boolean readBoolean() throws IOException {
    // inlined, shorter version of ensureBounds
    return (buf.readByte() & 0xff) == 1;
  }

  @Override
  public int readInt() throws IOException {
    int len = 1;
    int b = buf.readByte() & 0xff;
    int n = b & 0x7f;
    if (b > 0x7f) {
      b = buf.readByte() & 0xff;
      n ^= (b & 0x7f) << 7;
      if (b > 0x7f) {
        b = buf.readByte() & 0xff;
        n ^= (b & 0x7f) << 14;
        if (b > 0x7f) {
          b = buf.readByte() & 0xff;
          n ^= (b & 0x7f) << 21;
          if (b > 0x7f) {
            b = buf.readByte() & 0xff;
            n ^= (b & 0x7f) << 28;
            if (b > 0x7f) {
              throw new IOException("Invalid int encoding");
            }
          }
        }
      }
    }
    return (n >>> 1) ^ -(n & 1); // back to two's-complement
  }

  @Override
  public long readLong() throws IOException {
    int b = buf.readByte() & 0xff;
    int n = b & 0x7f;
    long l;
    if (b > 0x7f) {
      b = buf.readByte() & 0xff;
      n ^= (b & 0x7f) << 7;
      if (b > 0x7f) {
        b = buf.readByte() & 0xff;
        n ^= (b & 0x7f) << 14;
        if (b > 0x7f) {
          b = buf.readByte() & 0xff;
          n ^= (b & 0x7f) << 21;
          if (b > 0x7f) {
            // only the low 28 bits can be set, so this won't carry
            // the sign bit to the long
            l = innerLongDecode((long)n);
          } else {
            l = n;
          }
        } else {
          l = n;
        }
      } else {
        l = n;
      }
    } else {
      l = n;
    }
    return (l >>> 1) ^ -(l & 1); // back to two's-complement
  }
  
  // splitting readLong up makes it faster because of the JVM does more
  // optimizations on small methods
  private long innerLongDecode(long l) throws IOException {
    int len = 1;
    int b = buf.readByte() & 0xff;
    l ^= (b & 0x7fL) << 28;
    if (b > 0x7f) {
      b = buf.readByte() & 0xff;
      l ^= (b & 0x7fL) << 35;
      if (b > 0x7f) {
        b = buf.readByte() & 0xff;
        l ^= (b & 0x7fL) << 42;
        if (b > 0x7f) {
          b = buf.readByte() & 0xff;
          l ^= (b & 0x7fL) << 49;
          if (b > 0x7f) {
            b = buf.readByte() & 0xff;
            l ^= (b & 0x7fL) << 56;
            if (b > 0x7f) {
              b = buf.readByte() & 0xff;
              l ^= (b & 0x7fL) << 63;
              if (b > 0x7f) {
                throw new IOException("Invalid long encoding");
              }
            }
          }
        }
      }
    }
    return l;
  }

  @Override
  public float readFloat() throws IOException {
    int len = 1;
    int n = (buf.readByte() & 0xff) | ((buf.readByte() & 0xff) << 8)
        | ((buf.readByte() & 0xff) << 16) | ((buf.readByte() & 0xff) << 24);
    return Float.intBitsToFloat(n);
  }

  @Override
  public double readDouble() throws IOException {
    int len = 1;
    int n1 = (buf.readByte() & 0xff) | ((buf.readByte() & 0xff) << 8)
        | ((buf.readByte() & 0xff) << 16) | ((buf.readByte() & 0xff) << 24);
    int n2 = (buf.readByte() & 0xff) | ((buf.readByte() & 0xff) << 8)
        | ((buf.readByte() & 0xff) << 16) | ((buf.readByte() & 0xff) << 24);
    return Double.longBitsToDouble((((long) n1) & 0xffffffffL)
        | (((long) n2) << 32));
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    int length = readInt();
    Utf8 result = (old != null ? old : new Utf8());
    result.setByteLength(length);
    if (0 != length) {
      buf.readBytes(result.getBytes(), 0, length);
    }
    return result;
  }
  
  private final Utf8 scratchUtf8 = new Utf8();

  @Override
  public String readString() throws IOException {
    return readString(scratchUtf8).toString();
  }

  @Override
  public void skipString() throws IOException {
    skipFixed(readInt());
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = readInt();
    ByteBuffer result;
    if (old != null && length <= old.capacity()) {
      result = old;
      result.clear();
    } else {
      result = ByteBuffer.allocate(length);
    }
    buf.readBytes(result.array(), result.position(), length);
    result.limit(length);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    skipFixed(readInt());
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    buf.readBytes(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    buf.skipBytes(length);
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  /**
   * Returns the number of items to follow in the current array or map. Returns
   * 0 if there are no more items in the current array and the array/map has
   * ended.
   * 
   * @throws IOException
   */
  protected long doReadItemCount() throws IOException {
    long result = readLong();
    if (result < 0) {
      readLong(); // Consume byte-count if present
      result = -result;
    }
    return result;
  }

  /**
   * Reads the count of items in the current array or map and skip those items,
   * if possible. If it could skip the items, keep repeating until there are no
   * more items left in the array or map. If items cannot be skipped (because
   * byte count to skip is not found in the stream) return the count of the
   * items found. The client needs to skip the items individually.
   * 
   * @return Zero if there are no more items to skip and end of array/map is
   *         reached. Positive number if some items are found that cannot be
   *         skipped and the client needs to skip them individually.
   * @throws IOException
   */
  private long doSkipItems() throws IOException {
    long result = readInt();
    while (result < 0) {
      long bytecount = readLong();
      skipFixed((int) bytecount);
      result = readInt();
    }
    return result;
  }

  @Override
  public long readArrayStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long arrayNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipArray() throws IOException {
    return doSkipItems();
  }

  @Override
  public long readMapStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long mapNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipMap() throws IOException {
    return doSkipItems();
  }

  @Override
  public int readIndex() throws IOException {
    return readInt();
  }

  /**
   * Returns true if the current BinaryDecoder is at the end of its source data and
   * cannot read any further without throwing an EOFException or other
   * IOException.
   * <p/>
   * Not all implementations of BinaryDecoder support isEnd(). Implementations that do
   * not support isEnd() will throw a
   * {@link java.lang.UnsupportedOperationException}.
   */
  public boolean isEnd() throws IOException {
    return buf.readableBytes() == 0;
  }

}
