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
import org.apache.avro.io.DirectBinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An abstract {@link Encoder} for Avro's binary encoding.
 * <p/>
 * To construct and configure instances, use {@link EncoderFactory}
 * 
 * @see EncoderFactory
 * @see BufferedBinaryEncoder
 * @see DirectBinaryEncoder
 * @see BlockingBinaryEncoder
 * @see Encoder
 * @see Decoder
 */
public class ByteBufEncoder extends Encoder {

  private ByteBuf buf;

  public ByteBufEncoder() {
  }

  public ByteBufEncoder(ByteBuf buf) {
    this.buf = buf;
  }

  public ByteBufEncoder setBuf(ByteBuf buf) {
    this.buf = buf;
    return this;
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    buf.writeByte(b ? 1 : 0);
  }

  /* buffering is slower for ints that encode to just 1 or
   * two bytes, and and faster for large ones.
   * (Sun JRE 1.6u22, x64 -server) */
  @Override
  public void writeInt(int n) throws IOException {
    int val = (n << 1) ^ (n >> 31);
    if ((val & ~0x7F) == 0) {
      buf.writeByte(val);
      return;
    } else if ((val & ~0x3FFF) == 0) {
      buf.writeByte(0x80 | val);
      buf.writeByte(val >>> 7);
      return;
    }
    int n1 = n;
    // move sign to low-order bit, and flip others if negative
    n1 = (n1 << 1) ^ (n1 >> 31);
    if ((n1 & ~0x7F) != 0) {
      buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
      n1 >>>= 7;
      if (n1 > 0x7F) {
        buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
        n1 >>>= 7;
        if (n1 > 0x7F) {
          buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
          n1 >>>= 7;
          if (n1 > 0x7F) {
            buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
            n1 >>>= 7;
          }
        }
      }
    }
    buf.writeByte((byte) n1);

  }

  private void writeRestInt(int n1) {
    // move sign to low-order bit, and flip others if negative
    n1 = (n1 << 1) ^ (n1 >> 31);
    if ((n1 & ~0x7F) != 0) {
      buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
      n1 >>>= 7;
      if (n1 > 0x7F) {
        buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
        n1 >>>= 7;
        if (n1 > 0x7F) {
          buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
          n1 >>>= 7;
          if (n1 > 0x7F) {
            buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
            n1 >>>= 7;
          }
        }
      }
    }
    buf.writeByte((byte) n1);
  }

  /* buffering is slower for writeLong when the number is small enough to
   * fit in an int.
   * (Sun JRE 1.6u22, x64 -server) */
  @Override
  public void writeLong(long n) throws IOException {
    long val = (n << 1) ^ (n >> 63); // move sign to low-order bit
    if ((val & ~0x7FFFFFFFL) == 0) {
      int i = (int) val;
      while ((i & ~0x7F) != 0) {
        buf.writeByte((byte) ((0x80 | i) & 0xFF));
        i >>>= 7;
      }
      buf.writeByte((byte) i);
      return;
    }
    long n1 = n;
    // move sign to low-order bit, and flip others if negative
    n1 = (n1 << 1) ^ (n1 >> 63);
    if ((n1 & ~0x7FL) != 0) {
      buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
      n1 >>>= 7;
      if (n1 > 0x7F) {
        buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
        n1 >>>= 7;
        if (n1 > 0x7F) {
          buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
          n1 >>>= 7;
          if (n1 > 0x7F) {
            buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
            n1 >>>= 7;
            if (n1 > 0x7F) {
              buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
              n1 >>>= 7;
              if (n1 > 0x7F) {
                buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
                n1 >>>= 7;
                if (n1 > 0x7F) {
                  buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
                  n1 >>>= 7;
                  if (n1 > 0x7F) {
                    buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
                    n1 >>>= 7;
                    if (n1 > 0x7F) {
                      buf.writeByte((byte) ((n1 | 0x80) & 0xFF));
                      n1 >>>= 7;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    buf.writeByte((byte) n1);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    int bits = Float.floatToRawIntBits(f);
    // hotspot compiler works well with this variant
    buf.writeByte((byte) ((bits) & 0xFF));
    buf.writeByte((byte) ((bits >>> 8) & 0xFF));
    buf.writeByte((byte) ((bits >>> 16) & 0xFF));
    buf.writeByte((byte) ((bits >>> 24) & 0xFF));
  }

  @Override
  public void writeDouble(double d) throws IOException {
    long bits = Double.doubleToRawLongBits(d);
    int first = (int)(bits & 0xFFFFFFFF);
    int second = (int)((bits >>> 32) & 0xFFFFFFFF);
    // the compiler seems to execute this order the best, likely due to
    // register allocation -- the lifetime of constants is minimized.
    buf.writeByte((byte) ((first) & 0xFF));
    buf.writeByte((byte) ((second) & 0xFF));
    buf.writeByte((byte) ((second >>> 8) & 0xFF));
    buf.writeByte((byte) ((first >>> 8) & 0xFF));
    buf.writeByte((byte) ((first >>> 16) & 0xFF));
    buf.writeByte((byte) ((second >>> 16) & 0xFF));
    buf.writeByte((byte) ((second >>> 24) & 0xFF));
    buf.writeByte((byte) ((first >>> 24) & 0xFF));
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    buf.writeBytes(bytes, start, len);
  }

  protected void writeZero() throws IOException {
    buf.writeByte(0);
  }

  @Override
  public void writeNull() throws IOException {}

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    this.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
  }
  
  @Override
  public void writeString(String string) throws IOException {
    if (0 == string.length()) {
      writeZero();
      return;
    }
    byte[] bytes = string.getBytes("UTF-8");
    writeInt(bytes.length);
    writeFixed(bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    int len = bytes.limit() - bytes.position();
    if (0 == len) {
      writeZero();
    } else {
      writeInt(len);
      writeFixed(bytes);
    }
  }
  
  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    if (0 == len) {
      writeZero();
      return;
    }
    this.writeInt(len);
    this.writeFixed(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    this.writeInt(e);
  }

  @Override
  public void writeArrayStart() throws IOException {}

  @Override
  public void setItemCount(long itemCount) throws IOException {
    if (itemCount > 0) {
      this.writeLong(itemCount);
    }
  }
  
  @Override
  public void startItem() throws IOException {}

  @Override
  public void writeArrayEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeMapStart() throws IOException {}

  @Override
  public void writeMapEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    writeInt(unionIndex);
  }
  
}

