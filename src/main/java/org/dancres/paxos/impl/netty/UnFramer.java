package org.dancres.paxos.impl.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class UnFramer extends FrameDecoder {
    protected Object decode(ChannelHandlerContext aCtx, Channel aChannel, ChannelBuffer aBuffer) throws Exception {
        // Make sure if the length field was received.
        //
        if (aBuffer.readableBytes() < 4) {
            return null;
        }

			/*
			 * Mark the current buffer position before reading the length field
			 * because the whole frame might not be in the buffer yet. We will
			 * reset the buffer position to the marked position if there's not
			 * enough bytes in the buffer.
			 */
        aBuffer.markReaderIndex();

        // Read the length field.
        //
        int length = aBuffer.readInt();

        // Make sure if there's enough bytes in the buffer.
        //
        if (aBuffer.readableBytes() < length) {
				/*
				 * The whole bytes were not received yet - return null. This
				 * method will be invoked again when more packets are received
				 * and appended to the buffer.
				 *
				 * Reset to the marked position to read the length field again
				 * next time.
				 */
            aBuffer.resetReaderIndex();

            return null;
        }

        // There's enough bytes in the buffer. Read it.
        ChannelBuffer frame = aBuffer.readBytes(length);

        return frame.toByteBuffer();
    }
}

