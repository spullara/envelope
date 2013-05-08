package com.github.envelope;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * Reads a frame.
 */
@ChannelHandler.Sharable
public class FrameDecoder extends ByteToMessageDecoder<Frame> {
  private final static SpecificDatumReader<Frame> reader = new SpecificDatumReader<>(Frame.class);

  private ThreadLocal<ByteBufDecoder> decoder = new ThreadLocal<ByteBufDecoder>() {
    @Override
    protected ByteBufDecoder initialValue() {
      return new ByteBufDecoder();
    }
  };

  @Override
  public Frame decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf) throws Exception {
    int index = byteBuf.readerIndex();
    Decoder bd = decoder.get().setBuf(byteBuf);
    try {
      return reader.read(null, bd);
    } catch (IndexOutOfBoundsException eof) {
      // Wait for more data, reset reader index
      byteBuf.readerIndex(index);
      return null;
    }
  }
}
