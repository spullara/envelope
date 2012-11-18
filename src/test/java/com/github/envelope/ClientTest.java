package com.github.envelope;

import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientTest {

  private static final long CALLS = 10000000;

  private AtomicInteger i = new AtomicInteger(0);

  private ChannelFuture write(Channel ch) {
    i.incrementAndGet();
    return ch.write(new Frame(0l, 0, -1, newTags(), ByteBuffer.wrap("Hello, world!".getBytes())));
  }

  @Test
  public void testClient() throws InterruptedException {
    final SocketChannel ch = new NioSocketChannel();
    ChannelFuture register = new NioEventLoopGroup().register(ch);
    final Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();
    register.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        final long start = System.currentTimeMillis();
        ch.pipeline().addLast(new FrameEncoder(), new FrameDecoder(),
                new ChannelInboundMessageHandlerAdapter<Frame>() {
                  @Override
                  public void messageReceived(ChannelHandlerContext channelHandlerContext, Frame frame) throws Exception {
                    if (i.intValue() == CALLS) {
                      System.out.println(CALLS * 1000 / (System.currentTimeMillis() - start) + " calls per second");
                      semaphore.release();
                    } else {
                      write(ch);
                    }
                  }
                });
        ch.connect(new InetSocketAddress("localhost", 6380)).addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture channelFuture) throws Exception {
            write(ch);
          }
        });
      }
    });
    semaphore.acquire();
  }


  @Test
  public void testPipelinedClient() throws InterruptedException {
    final ExecutorService es = Executors.newCachedThreadPool();
    final SocketChannel ch = new NioSocketChannel();
    ChannelFuture register = new NioEventLoopGroup().register(ch);
    final Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();
    register.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        final Semaphore pipelines = new Semaphore(1000);
        DefaultEventExecutorGroup group = new DefaultEventExecutorGroup(1);
        ch.pipeline().addLast(group, new FrameEncoder(), new FrameDecoder(),
                new ChannelInboundMessageHandlerAdapter<Frame>() {
                  @Override
                  public void messageReceived(ChannelHandlerContext channelHandlerContext, Frame frame) throws Exception {
                    if (i.intValue() >= CALLS) {
                      semaphore.release();
                    }
                    pipelines.release();
                  }
                });
        ch.connect(new InetSocketAddress("localhost", 6380)).addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture channelFuture) throws Exception {
            es.submit(new Callable<Object>() {
              @Override
              public Object call() throws Exception {
                while (i.intValue() < CALLS) {
                  pipelines.acquire(1);
                  write(ch);
                }
                pipelines.acquire(100);
                return null;
              }
            });
          }
        });
      }
    });
    long start = System.currentTimeMillis();
    semaphore.acquire();
    System.out.println(CALLS * 1000l / (System.currentTimeMillis() - start) + " calls per second");
  }

  @Test
  public void testSimple() throws InterruptedException {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final SocketChannel ch = new NioSocketChannel();
    ch.pipeline().addLast(new FrameEncoder());
    new NioEventLoopGroup().register(ch).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        ch.connect(new InetSocketAddress("localhost", 6380)).addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture channelFuture) throws Exception {
            ch.write(new Frame(0l, 1, -1, newTags(), ByteBuffer.wrap("Hello, ".getBytes())));
            ch.write(new Frame(1l, 1, -1, newTags(), ByteBuffer.wrap("world".getBytes())));
            ch.write(new Frame(2l, 1, -1, newTags(), ByteBuffer.wrap("!".getBytes())));
            ch.write(new Frame(3l, 1, -1, newTags(), ByteBuffer.allocate(8192)));
            countDownLatch.countDown();
          }
        });
      }
    });
    countDownLatch.await();
    ch.close().sync();
  }

  private HashMap<CharSequence, CharSequence> newTags() {
    return new HashMap<CharSequence, CharSequence>();
  }
}
