package com.rie.LightingMQ.connection;

import com.rie.LightingMQ.config.ConnectionConfig;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.TransferType;
import com.rie.LightingMQ.util.codec.MarshallingCodeCFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/17.
 */
public class Client {

    private final static Logger LOGGER = LoggerFactory.getLogger(Client.class);
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final EventExecutorGroup eventExecutorGroup;
    private volatile boolean connected;
    private Map<Integer, RequestFuture> responseCache = new ConcurrentHashMap<>();
    private Channel channel;
    private static ConnectionConfig config;
    private SocketAddress localAddress;

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

    public static Client newClientInstance(ConnectionConfig config) {

        Client.config = config;
        Client client = newClientInstance(config.getHost(), config.getPort());
        return client;
    }

    public void init(String host, int port) {

        if (!connected) {
            this.bootstrap.group(this.eventLoopGroup).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(
                                    eventExecutorGroup,
//                                    new LoggingHandler(LogLevel.ERROR),
                                    //心跳
                                    new IdleStateHandler(0, 0, config.getAllIdleTime(), TimeUnit.MILLISECONDS),
                                    //decode
                                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
                                    MarshallingCodeCFactory.newMarshallingDecoder(),

                                    //encode
                                    new LengthFieldPrepender(4),
                                    MarshallingCodeCFactory.newMarshallingEncoder(),

                                    new ClientHandler()
                            );
                        }
                    });
            ChannelFuture future = bootstrap.connect(host, port);
            this.channel = future.channel();
            try {
                future.sync();
                localAddress = channel.localAddress();
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
    }

    class ClientHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        protected void messageReceived(ChannelHandlerContext channelHandlerContext, Message message) throws Exception {

            if (message.getType() == TransferType.HEARTBEAT.value) { //心跳信息
                LOGGER.info("client receive heartbeat message from server.");
            }
            else if (message.getType() == TransferType.EXCEPTION.value
                    || message.getType() == TransferType.REPLY.value) {
                int id = message.getSeqId();
                RequestFuture responseFuture = responseCache.get(id);
                if (responseFuture != null) { //发送量过大导致待处理future缓存溢出
                    responseFuture.setResponse(message);
                    responseFuture.release(); //通知接受者
                }
                else {
                    LOGGER.warn("request for response {} in cache was missing.", message);
                }
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                switch(event.state()) {
                    case READER_IDLE:
                        LOGGER.info("---READER_IDLE---");
                        break;
                    case WRITER_IDLE:
                        LOGGER.info("---WRITER_IDLE---");
                        break;
                    case ALL_IDLE:  //发送心跳
                        LOGGER.info("---ALL_IDLE---");
                        Message heartbeatMsg = Message.newHeartbeatMessage();
                        LOGGER.info("send heartbeat message to server.");
                        ctx.writeAndFlush(heartbeatMsg).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                if (channelFuture.isSuccess()) {
                                    LOGGER.info("send heartbeat message success.");
                                } else {
                                    LOGGER.info("send heartbeat message failed.");
                                    if (!reConnect()) {
                                        stop();
                                    }
                                }
                            }
                        });
                        break;
                    default:
                        break;
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {

            LOGGER.info("connected to server {}.", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {

            Client.this.connected = false; //设置连接状态
            LOGGER.info("disconnected to server {}.", ctx.channel().remoteAddress());
        }
    }

    public RequestFuture write(final Message request) {

        final RequestFuture response = new RequestFuture(request.getSeqId());
        responseCache.put(request.getSeqId(), response); //异步发送 对future进行缓冲

        if (channel.isActive()) {
            this.channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()) { //发送成功
                        response.setSucceed_send(true);
                        return;
                    }
                    responseCache.remove(response.getId());  //发送失败 future从缓存中删除
                    response.setSucceed_send(false);
                    response.setCause(channelFuture.cause());
                    LOGGER.warn("send the request({}) to {} failed.({})", request, channelFuture.channel().remoteAddress());
                    if (!reConnect()) {
                        stop();
                    }
                }
            });
        }
        return response;
    }

    public SocketAddress getLocalAddress() {
        return localAddress;
    }

    public Channel getChannel() {
        return channel;
    }

    public boolean reConnect() {

        int reConCounter = 0;
        while (!connected && reConCounter < config.getReConnectTimes()) {  //重新连接
            try {
                ChannelFuture future = bootstrap.connect(config.getHost(), config.getPort());
                TimeUnit.SECONDS.sleep(5);
                this.channel = future.channel();
                if (channel.isActive()) {
                    connected = true;
                    localAddress = channel.localAddress();
                }
                reConCounter++;
            } catch (InterruptedException e) {
                LOGGER.warn("interrupted while reconnect server.");
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return connected;
    }
}
