package org.dancres.paxos.impl.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import org.dancres.paxos.NodeId;
import org.dancres.paxos.Stream;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.NetworkUtils;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportImpl extends SimpleChannelHandler implements Transport {
	private static Logger _logger = LoggerFactory
			.getLogger(TransportImpl.class);

	public static final int BROADCAST_PORT = 41952;

	private static InetSocketAddress _mcastAddr;
	private DatagramChannelFactory _mcastFactory;
	private DatagramChannel _mcast;
	private DatagramChannelFactory _unicastFactory;
	private DatagramChannel _unicast;
	private NioServerSocketChannelFactory _serverStreamFactory; 
	private ServerSocketChannel _serverStreamChannel;
	private NioClientSocketChannelFactory _clientStreamFactory;
	private Dispatcher _dispatcher;
	private NodeId _nodeId;
	
	public interface Dispatcher {
		public void setTransport(Transport aTransport);
		public void messageReceived(PaxosMessage aMessage);
	}
	
	public TransportImpl(Dispatcher aDispatcher) throws Exception {
		InetSocketAddress myMcastTarget = new InetSocketAddress((InetAddress) null,
				BROADCAST_PORT);
		_mcastAddr = new InetSocketAddress("224.0.0.1", BROADCAST_PORT);

		_mcastFactory = new OioDatagramChannelFactory(Executors.newCachedThreadPool());

		_mcast = _mcastFactory.newChannel(newPipeline());
		ChannelFuture myFuture = _mcast.bind(myMcastTarget);
		myFuture.await();
		_mcast.joinGroup(_mcastAddr.getAddress());
		
		_unicastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		
		_unicast = _unicastFactory.newChannel(newPipeline());
		myFuture = _unicast.bind(new InetSocketAddress(NetworkUtils.getWorkableInterface(), 0));
		myFuture.await();
		
		_nodeId = NodeId.from(_unicast.getLocalAddress());
		
		_logger.info("Transport bound on: " + _nodeId);

		_serverStreamFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), 
				Executors.newCachedThreadPool());
		
		_serverStreamChannel = _serverStreamFactory.newChannel(newPipeline());
		_serverStreamChannel.bind(_unicast.getLocalAddress());
		_serverStreamChannel.getConfig().setPipelineFactory(Channels.pipelineFactory(newPipeline()));
		
		_clientStreamFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool());
		
		synchronized(this) {
			_dispatcher = aDispatcher;
			_dispatcher.setTransport(this);
		}
	}

	public void shutdown() {
		try {
			_logger.debug("Unbind mcast");
			ChannelFuture myFuture = _mcast.unbind();
			myFuture.await();

			_logger.debug("Close unicast");
			myFuture = _unicast.close();
			myFuture.await();

			_logger.debug("Unbind unicast");
			myFuture = _unicast.unbind();
			myFuture.await();

			_logger.debug("Close serverstream");
			myFuture = _serverStreamChannel.close();
			myFuture.await();
			
			_logger.debug("Unbind serverstream");
			myFuture = _serverStreamChannel.unbind();
			myFuture.await();
			
			_logger.debug("Stop mcast factory");
			_mcastFactory.releaseExternalResources();

			_logger.debug("Stop unicast factory");
			_unicastFactory.releaseExternalResources();

			_logger.debug("Stop serverstream factory");
			_serverStreamFactory.releaseExternalResources();
			
			_logger.debug("Stop clientstream factory");
			_clientStreamFactory.releaseExternalResources();			
			
			_logger.debug("Shutdown complete");
		} catch (Exception anE) {
			_logger.error("Failed to shutdown cleanly", anE);
		}

	}
	
	private ChannelPipeline newPipeline() {
		ChannelPipeline myPipeline = Channels.pipeline();
		myPipeline.addLast("framer", new Framer());
		myPipeline.addLast("unframer", new Unframer());
		myPipeline.addLast("encoder", new Encoder());
		myPipeline.addLast("decoder", new Decoder());
		myPipeline.addLast("transport", this);
		
		return myPipeline;
	}
	
	public NodeId getLocalNodeId() {
		return _nodeId;
	}

	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		_logger.info("Connected: " + ctx + ", " + e);
	}
	
	public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
		_logger.info("Stream open: " + ctx + ", " + e);		
	}
	
    public void messageReceived(ChannelHandlerContext aContext, MessageEvent anEvent) {
    	Dispatcher myDispatcher;
    	
    	synchronized(this) {
    		myDispatcher = _dispatcher;
    	}
    	
    	if (myDispatcher != null)
    		myDispatcher.messageReceived((PaxosMessage) anEvent.getMessage());
    }

    public void exceptionCaught(ChannelHandlerContext aContext, ExceptionEvent anEvent) {
        _logger.error("Problem in transport", anEvent.getCause());
        anEvent.getChannel().close();
    }		
	
	public void send(PaxosMessage aMessage, NodeId aNodeId) {
		try {
			if (aNodeId.equals(NodeId.BROADCAST))
				_mcast.write(aMessage, _mcastAddr);
			else {
				_unicast.write(aMessage, NodeId.toAddress(aNodeId));
			}
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
	}

	private static class StreamImpl implements Stream {
		private SocketChannel _channel;
		
		StreamImpl(SocketChannel aChannel) {
			_channel = aChannel;
		}
		
		public void close() {
			ChannelFuture myFuture = _channel.close();
			
			try {
				myFuture.await();
			} catch (InterruptedException anIE) {
			}
		}
		
		public void send(PaxosMessage aMessage) {
			_channel.write(aMessage);
		}
	}
	
	public Stream connectTo(NodeId aNodeId) {
		SocketChannel myChannel = _clientStreamFactory.newChannel(newPipeline());
		
		try {
			ChannelFuture myFuture = myChannel.connect(NodeId.toAddress(aNodeId));
			myFuture.await();
			
			return new StreamImpl(myChannel);
		} catch (Exception anE) {
			_logger.error("Couldn't connect to: " + Long.toHexString(aNodeId.asLong()), anE);
			return null;
		}
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
	
	public static void main(String[] anArgs) throws Exception {
		Transport _tport1 = new TransportImpl(new DispatcherImpl());
		Transport _tport2 = new TransportImpl(new DispatcherImpl());
		
		_tport1.send(new Accept(1, 2, _tport1.getLocalNodeId().asLong()), NodeId.BROADCAST);
		_tport1.send(new Accept(2, 3, _tport2.getLocalNodeId().asLong()), _tport2.getLocalNodeId());
		
		Thread.sleep(5000);
		_tport1.shutdown();
		_tport2.shutdown();
	}
	
	static class DispatcherImpl implements Dispatcher {
		public void messageReceived(PaxosMessage aMessage) {
			System.err.println("Message received: " + aMessage);
		}

		public void setTransport(Transport aTransport) {
			System.err.println("Dispatcher " + this + " got transport: " + aTransport);
		}		
	}
}
