package com.rie.LightingMQ.util.codec;

import io.netty.handler.codec.marshalling.*;
import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;

/**
 * Created by Charley on 2017/7/18.
 */
public final class MarshallingCodeCFactory {

    public static MarshallingDecoder newMarshallingDecoder() {

        final MarshallerFactory factory = Marshalling.getProvidedMarshallerFactory("serial");
        final MarshallingConfiguration configuration = new MarshallingConfiguration();
        configuration.setVersion(5);
        UnmarshallerProvider provider = new DefaultUnmarshallerProvider(factory, configuration);
        Decoder decoder = new Decoder(provider, 1024);
        return decoder;
    }

    public static MarshallingEncoder newMarshallingEncoder() {

        final MarshallerFactory factory = Marshalling.getProvidedMarshallerFactory("serial");
        final MarshallingConfiguration configuration = new MarshallingConfiguration();
        configuration.setVersion(5);
        MarshallerProvider provider = new DefaultMarshallerProvider(factory, configuration);
        Encoder encoder = new Encoder(provider);
        return encoder;

    }
}
