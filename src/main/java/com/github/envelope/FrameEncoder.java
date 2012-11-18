package com.github.envelope;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Write a frame.
 */
@ChannelHandler.Sharable
public class FrameEncoder extends MessageToByteEncoder<Frame> {
  private static final SpecificDatumWriter<Frame> writer = new SpecificDatumWriter<>(Frame.class);

  @Override
  public void encode(ChannelHandlerContext channelHandlerContext, Frame frame, ByteBuf byteBuf) throws Exception {
    writer.write(frame, new ByteBufEncoder(byteBuf));
  }
}
