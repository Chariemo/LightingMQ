package com.rie.LightingMQ.connection;

import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.util.codec.MarshallingCodecFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Charley on 2017/7/17.
 */
public class Client {

    private final static Logger LOGGER = LoggerFactory.getLogger(Client.class);
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final EventExecutorGroup eventExecutorGroup;
    private boolean connected;
    private Map<Integer, ResponseFuture> responseCache = new ConcurrentHashMap<>();
    private Channel channel;

    public Client() {

        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = new NioEventLoopGroup();
        this.eventExecutorGroup = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors());
    }

    public static Client newClientInstance(String host, int port) {

        Client client = new Client();
        client.init(host, port);
        return client;
    }

    public void init(String host, int port) {

        if (!connected) {
            this.bootstrap.group(this.eventLoopGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(
                                    eventExecutorGroup,
                                    MarshallingCodecFactory.newMarshallingDecoder(),
                                    MarshallingCodecFactory.newMarshallingEncoder(),
                                    new ClientHandler()
                            );
                        }
                    });
            ChannelFuture future = bootstrap.connect(host, port);
            this.channel = future.channel();
            try {
                future.sync();
                LOGGER.info("connect {}:{} successfully.", host, port);
                connected = true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public void stop() {

        LOGGER.info("closing channel:{}.", this.channel);
        if (this.eventLoopGroup != null) {
            this.eventLoopGroup.shutdownGracefully();
        }
        if (this.eventExecutorGroup != null) {
            this.eventExecutorGroup.shutdownGracefully();
        }
        if (this.channel != null) {
            this.channel.close();
        }
        connected = false;
        LOGGER.info("channel:{} has closed.", this.channel);
    }

    class ClientHandler extends SimpleChannelInboundHandler<Message> {

       /* @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {

            ctx.writeAndFlush(Message.newRequestMessage()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {

                    if (channelFuture.isSuccess()) {
                        System.out.println("send");
                    }
                    else {
                        System.out.println("false");
                    }
                }
            });
        }*/

        @Override
        protected void messageReceived(ChannelHandlerContext channelHandlerContext, Message message) throws Exception {

            int id = message.getSeqId();
            ResponseFuture responseFuture = responseCache.get(id);
            if (responseFuture != null) {
                System.out.println("receive response");
                responseFuture.setSucceed_recieved(true);
                responseFuture.setResponse(message);
                responseFuture.release();
            }
            else {

            }
        }

    }

    public ResponseFuture write(final Message request) {

        final ResponseFuture response = new ResponseFuture(request.getSeqId());
        responseCache.put(request.getSeqId(), response);

        if (channel.isActive()) {
            this.channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) {
                        response.setSucceed_send(true);
                        return;
                    }
                    responseCache.remove(response.getId());
                    response.setSucceed_send(false);
                    response.setCause(channelFuture.cause());
                    LOGGER.warn("send the request to <{}> failed.({})", channelFuture.channel(), request);
                }
            });
        }
        return response;
    }



}
