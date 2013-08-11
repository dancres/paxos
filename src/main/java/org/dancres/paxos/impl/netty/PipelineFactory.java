package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.jboss.netty.channel.ChannelPipeline;

public interface PipelineFactory {
    ChannelPipeline newPipeline(Transport.PacketPickler aPickler);
}
