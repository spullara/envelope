package com.github.envelope;

import io.netty.channel.ChannelHandlerContext;

public interface Service<T> {
  void handle(ChannelHandlerContext ctx, T message);
}
