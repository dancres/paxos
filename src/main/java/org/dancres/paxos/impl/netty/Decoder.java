package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import java.nio.ByteBuffer;

class Decoder extends OneToOneDecoder {
    private final Transport.PacketPickler _pickler;

    public Decoder(Transport.PacketPickler aPickler) {
        _pickler = aPickler;
    }

    protected Object decode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
        ByteBuffer myBuffer = (ByteBuffer) anObject;
        return _pickler.unpickle(myBuffer.array());
    }
}

