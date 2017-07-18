package com.rie.LightingMQ.broker;

import com.rie.LightingMQ.config.ServerConfig;
import com.rie.LightingMQ.util.MarshallingCodecFactory;
import com.rie.LightingMQ.util.PortScanUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Charley on 2017/7/18.
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);
    private EventLoopGroup baseLoopGroup;
    private EventLoopGroup workerLoopGroup;
    private ServerBootstrap serverBootstrap;
    private ServerConfig config;
    private ChannelFuture channelFuture;
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

    public void init() {

        LOGGER.info("Server is starting.");
        this.baseLoopGroup = new NioEventLoopGroup();
        this.workerLoopGroup = new NioEventLoopGroup();
        this.serverBootstrap = new ServerBootstrap();

        serverBootstrap.group(baseLoopGroup, workerLoopGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_TIMEOUT, 6000)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
                socketChannel.pipeline().addLast(
                        MarshallingCodecFactory.newMarshallingDecoder(),
                        MarshallingCodecFactory.newMarshallingEncoder()
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
                channelFuture.channel().closeFuture().sync();
            }
        } catch (InterruptedException e) {
            LOGGER.error("Exception happen to start server: {}", e);
            throw new RuntimeException(e.getMessage(), e);
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
        }
    }

    class ShutdownThread extends Thread {

        public void run() {
            Server.this.stop();
        }
    }

}
