package org.dancres.paxos.impl.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public class MulticastTest {
	public static void main(String[] anArgs) throws Exception {
		InetSocketAddress myTarget = new InetSocketAddress((InetAddress) null, 41952);
		InetSocketAddress myMcast = new InetSocketAddress("224.0.0.1", 41952);
		
		DatagramChannelFactory myFactory = new OioDatagramChannelFactory(Executors.newCachedThreadPool());
		
		DatagramChannel[] myChannels = new DatagramChannel[3];

		for (int i = 0; i < myChannels.length; i++) {
			ChannelPipeline myPipeline = Channels.pipeline();
			myPipeline.addLast("framer", new Decoder());
			myPipeline.addLast("Reader", new Receiver());
			
			myChannels[i] = myFactory.newChannel(myPipeline);
			ChannelFuture myFuture = myChannels[i].bind(myTarget);
			myFuture.await();
			myChannels[i].joinGroup(myMcast.getAddress());
		}

		ChannelBuffer myBuffer = ChannelBuffers.buffer(4);
		myBuffer.writeInt(45);
		
		while (true) {
			ChannelFuture myWriteFuture = myChannels[0].write(myBuffer, myMcast);
			myWriteFuture.await();
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException anIE) {
				// Doesn't matter
			}
		}
	}
	
	private static class Receiver extends SimpleChannelHandler {
	    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
	        ChannelBuffer m = (ChannelBuffer) e.getMessage();
	        
	        if (m.readableBytes() >= 4) {
	        	System.out.println(this + " got " + m.readInt());
	        }
	    }

	    @Override
	    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
	        e.getCause().printStackTrace();
	        e.getChannel().close();
	    }		
	}
	
	private static class Decoder extends FrameDecoder {

	    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) {

			if (buffer.readableBytes() < 4) {
				return null;
			}

			return buffer.readBytes(4);
	    }
	}	
}
