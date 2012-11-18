package com.github.envelope;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventExecutorGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Redis server
 */
public class Server {
  @Argument(alias = "p")
  private static Integer port = 6380;

  public static void main(String[] args) throws InterruptedException {
    try {
      Args.parse(Server.class, args);
    } catch (IllegalArgumentException e) {
      Args.usage(Server.class);
      System.exit(1);
    }

    // Configure the server
    final Server server = new Server();

    // Echo service
    server.addService(0, new Service<Frame>() {
      @Override
      public void handle(ChannelHandlerContext ctx, Frame message) {
        ctx.write(message);
      }
    });

    // Configure the network
    ServerBootstrap b = new ServerBootstrap();
    final FrameDecoder frameDecoder = new FrameDecoder();
    final FrameEncoder frameEncoder = new FrameEncoder();
    final FrameHandler frameHandler = new FrameHandler(server);
    final DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
    try {
        b.group(new NioEventLoopGroup(), new NioEventLoopGroup())
         .channel(NioServerSocketChannel.class)
         .option(ChannelOption.SO_BACKLOG, 100)
         .localAddress(port)
         .childOption(ChannelOption.TCP_NODELAY, true)
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
               ch.pipeline().addLast(group, frameDecoder, frameEncoder, frameHandler);
             }
         });

        // Start the server and wait until the server socket is closed.
        b.bind().sync().channel().closeFuture().sync();
    } finally {
        // Shut down all event loops to terminate all threads.
        b.shutdown();
    }
  }

  public void addService(int channel, Service service) {
    services.put(channel, service);
  }

  private final Logger log = Logger.getLogger("Server");
  private final Map<Integer, Service<Frame>> services = new ConcurrentHashMap<>();

  public void received(ChannelHandlerContext ctx, Frame frame) {
    Service<Frame> service = services.get(frame.getChannel());
    if (service == null) {
      log.warning("Received message on unregistered channel: " + frame);
    } else {
      service.handle(ctx, frame);
    }
  }
}
