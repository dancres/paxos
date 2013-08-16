package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;

public class DefaultPipelineFactory implements PipelineFactory {
    public ChannelPipeline newPipeline(Transport.PacketPickler aPickler, SimpleChannelHandler aHandler) {
        ChannelPipeline myPipeline = Channels.pipeline();
        myPipeline.addLast("framer", new Framer());
        myPipeline.addLast("unframer", new UnFramer());
        myPipeline.addLast("encoder", new Encoder(aPickler));
        myPipeline.addLast("decoder", new Decoder(aPickler));
        myPipeline.addLast("transport", aHandler);

        return myPipeline;
    }
}