package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.SimpleChannelHandler;

public interface PipelineFactory {
    ChannelPipeline newPipeline(Transport.PacketPickler aPickler, SimpleChannelHandler aHandler);
}
