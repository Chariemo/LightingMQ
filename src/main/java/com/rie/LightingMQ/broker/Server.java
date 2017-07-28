package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.broker.requestHandlers.DefaultFetchRequestHandler;
import com.rie.LightingMQ.broker.requestHandlers.DefaultPrePublishRequestHandler;
import com.rie.LightingMQ.broker.requestHandlers.DefaultPublishRequestHandler;
import com.rie.LightingMQ.config.ServerConfig;
import com.rie.LightingMQ.message.Message;
import com.rie.LightingMQ.message.TransferType;
import com.rie.LightingMQ.storage.TopicQueuePool;
import com.rie.LightingMQ.util.codec.MarshallingCodeCFactory;
import com.rie.LightingMQ.util.PortScanUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Charley on 2017/7/18.
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    private static final RequestHandler DEFAULT_FETCH = new DefaultFetchRequestHandler();
    private static final RequestHandler DEFAULT_PUBLISH = new DefaultPublishRequestHandler();
    private static final RequestHandler DEFAULT_PRE_PUBLISH = new DefaultPrePublishRequestHandler();
    private EventLoopGroup baseLoopGroup;
    private EventLoopGroup workerLoopGroup;
    private ServerBootstrap serverBootstrap;
    private ServerConfig config;
    private ChannelFuture channelFuture;
    private IntObjectMap<RequestHandler> requestHandlers;
    private boolean started;

    public Server() {

    }

    public Server(ServerConfig config) {

        this.config = config;
    }

    public static Server newServerInstance(ServerConfig config) {

        Server server = new Server(config);
        server.init();
        return server;
    }

    public static Server newServerInstance(Properties properties) {

        return newServerInstance(new ServerConfig(properties));
    }

    public static Server newServerInstance(String configPath) {

        return newServerInstance(new ServerConfig(configPath));
    }

    public static Server newServerInstance() {

        return newServerInstance(ServerConfig.getDefaultServerConfig());
    }

    public void init() {

        LOGGER.info("Server is starting.");
        this.baseLoopGroup = new NioEventLoopGroup();
        this.workerLoopGroup = new NioEventLoopGroup();
        this.serverBootstrap = new ServerBootstrap();
        this.requestHandlers = new IntObjectHashMap();
        requestHandlers.put(RequestHandlerType.FETCH.value, DEFAULT_FETCH);
        requestHandlers.put(RequestHandlerType.PUBLISH.value, DEFAULT_PUBLISH);
        requestHandlers.put(RequestHandlerType.PRE_PUBLISH.value, DEFAULT_PRE_PUBLISH);

        serverBootstrap.group(baseLoopGroup, workerLoopGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_TIMEOUT, 6000)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        serverBootstrap.handler(new LoggingHandler(LogLevel.ERROR))
                .childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
                socketChannel.pipeline().addLast(
                        //心跳
                        new IdleStateHandler(config.getReadIdleTime(), 0, 0, TimeUnit.MILLISECONDS),

                        /*decode*/
                        //tcp粘包处理
                        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
                        //消息解码
                        MarshallingCodeCFactory.newMarshallingDecoder(),

                        //encode
                        new LengthFieldPrepender(4),
                        MarshallingCodeCFactory.newMarshallingEncoder(),

                        new ServerHandler()
                );
            }
        });


        try {
            if (PortScanUtil.checkAvailablePort(config.getPort())) {
                if (config.getHost() != null) {
                    channelFuture = serverBootstrap.bind(config.getHost(), config.getPort()).sync();
                }
                else {
                    channelFuture = serverBootstrap.bind(config.getPort()).sync();
                }
                this.started = true;
                LOGGER.info("server: {} has started.", channelFuture.channel());
                Runtime.getRuntime().addShutdownHook(new ShutdownThread());

                TopicQueuePool.singletonInstance(config);

                channelFuture.channel().closeFuture().sync();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Exception happen to start server: {}", e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void registerHandler(int handlerId, RequestHandler handler) {

        requestHandlers.put(handlerId, handler);
    }

    class ServerHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        protected void messageReceived(final ChannelHandlerContext channelHandlerContext, final Message message) throws Exception {

            Message response = null;

            if (message.getType() == TransferType.HEARTBEAT.value) {  //心跳信息
                LOGGER.info("server receive heartbeat from client.");
                response = Message.newHeartbeatMessage();
                channelHandlerContext.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        if (channelFuture.isSuccess()) {
                            LOGGER.info("send response for heartbeat message succeed");
                        }
                        else {
                            LOGGER.warn("send response for heartbeat message failed");
                        }
                    }
                });
            }
            else {
                RequestHandler handler = requestHandlers.get(message.getReqHandlerType());
                if (handler != null) {
                    response = handler.requestHandle(message);
                    if (response != null) {
                        channelHandlerContext.writeAndFlush(response).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                if (channelFuture.isSuccess()) {
                                    LOGGER.info("send response for ({}) to {} succeed.", message, channelHandlerContext.channel().remoteAddress());
                                }
                                else {
                                    LOGGER.info("send response for ({}) to {} failed.", message, channelHandlerContext.channel().remoteAddress());
                                }
                            }
                        });
                    }
                }
                else {
                    response = Message.newExceptionMessage();
                    response.setSeqId(message.getSeqId());
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {

            LOGGER.info("client {} has connected.", ctx.channel().remoteAddress());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {

            LOGGER.info("client {} has closed.", ctx.channel().remoteAddress());
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {

            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                switch (event.state()) {
                    case READER_IDLE: // 客户端规定时间无响应
                        LOGGER.info("---READER_IDLE---");
                        ctx.close();
                        break;
                    default:
                        break;
                }
            }
        }
    }


    public void stop() {

        if (this.started) {
            LOGGER.info("The Server:{} is stopping.", channelFuture.channel());
            baseLoopGroup.shutdownGracefully();
            workerLoopGroup.shutdownGracefully();
            if (channelFuture.channel().isActive()) {
                channelFuture.channel().close();
            }
            LOGGER.info("The Server:{} has stopped.", channelFuture.channel());

            TopicQueuePool.close();
        }
    }

    class ShutdownThread extends Thread {

        public void run() {
            Server.this.stop();
        }
    }


}
