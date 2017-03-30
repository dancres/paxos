package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.ByteBuffer;

class Encoder extends OneToOneEncoder {
    private final Transport.PacketPickler _pickler;

    public Encoder(Transport.PacketPickler aPickler) {
        _pickler = aPickler;
    }

    protected Object encode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
        PacketImpl myPacket = (PacketImpl) anObject;

        return ByteBuffer.wrap(_pickler.pickle(myPacket));
    }
}
