package org.dancres.paxos.impl.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import java.nio.ByteBuffer;

class Framer extends OneToOneEncoder {
    protected Object encode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
        ChannelBuffer myBuff = ChannelBuffers.dynamicBuffer();
        ByteBuffer myMessage = (ByteBuffer) anObject;

        myBuff.writeInt(myMessage.limit());
        myBuff.writeBytes(myMessage.array());

        return myBuff;
    }
}

