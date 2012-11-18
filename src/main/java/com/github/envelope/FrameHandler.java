package com.github.envelope;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;

import java.io.EOFException;

/**
 * Write a frame.
 */
@ChannelHandler.Sharable
public class FrameHandler extends ChannelInboundMessageHandlerAdapter<Frame> {
  private final Server server;

  public FrameHandler(Server server) {
    super(Frame.class);
    this.server = server;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (!(cause.getCause() instanceof EOFException)) {
      cause.printStackTrace();
    }
    ctx.close();
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, Frame frame) throws Exception {
    server.received(ctx, frame);
  }
}
