package com.rie.LightingMQ.connection;

import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.util.MarshallingCodecFactory;
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
    private Map<Integer, MessageFuture> responseCache = new ConcurrentHashMap<>();
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
                LOGGER.info("connect {}:{} succeessfully.", host, port);
                connected = true;
                channel.closeFuture().sync();
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
        @Override
        protected void messageReceived(ChannelHandlerContext channelHandlerContext, Message message) throws Exception {

            int id = message.getSeqId();
            MessageFuture messageFuture = responseCache.get(id);
            if (messageFuture != null) {
                messageFuture.setSucceed_recieved(true);
                messageFuture.setResponse(message);
                messageFuture.release();
            }
            else {

            }
        }
    }

    public MessageFuture write(final Message request) {

        final MessageFuture response = new MessageFuture(request.getSeqId());
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
