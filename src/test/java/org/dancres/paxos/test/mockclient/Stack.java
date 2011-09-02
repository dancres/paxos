package org.dancres.paxos.test.mockclient;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

public class Stack {
	static ChannelPipeline newPipeline(SimpleChannelHandler aHandler) {
		ChannelPipeline myPipeline = Channels.pipeline();
		myPipeline.addLast("framer", new Framer());
		myPipeline.addLast("unframer", new Unframer());
		myPipeline.addLast("encoder", new Encoder());
		myPipeline.addLast("decoder", new Decoder());
		myPipeline.addLast("transport", aHandler);
		
		return myPipeline;
	}
		
	private static class Encoder extends OneToOneEncoder {
		protected Object encode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
			PaxosMessage myMsg = (PaxosMessage) anObject;
			
			return ByteBuffer.wrap(Codecs.encode(myMsg));
		}
	}
	
	private static class Decoder extends OneToOneDecoder {
		protected Object decode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
			ByteBuffer myBuffer = (ByteBuffer) anObject;
			PaxosMessage myMessage = Codecs.decode(myBuffer.array());
			
			return myMessage;
		}		
	}
	
	private static class Framer extends OneToOneEncoder {
		protected Object encode(ChannelHandlerContext aCtx, Channel aChannel, Object anObject) throws Exception {
			ChannelBuffer myBuff = ChannelBuffers.dynamicBuffer();
			ByteBuffer myMessage = (ByteBuffer) anObject;

			myBuff.writeInt(myMessage.limit());
			myBuff.writeBytes(myMessage.array());

			return myBuff;
		}
	}

	private static class Unframer extends FrameDecoder {
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
}
