package org.dancres.paxos.test.mockclient;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.rmi.CORBA.Util;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.discovery.*;
import org.dancres.paxos.impl.CheckpointHandle;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Backend extends SimpleChannelHandler {
	private static final String HANDBACK_KEY = "org.dancres.paxos.test.backend.handback";
	
	private static Logger _logger = LoggerFactory
			.getLogger(Backend.class);

	private InetSocketAddress _unicastAddr;
	private DatagramChannelFactory _unicastFactory;
	private DatagramChannel _unicast;
    private ChannelGroup _channels = new DefaultChannelGroup();
    private Paxos _paxos;
    private Registrar _registrar;
    private AtomicLong _handbackGenerator = new AtomicLong(0);
	private Map<String, InetSocketAddress> _requestMap = new ConcurrentHashMap<String, InetSocketAddress>();

	
	public static void main(String[] anArgs) throws Exception {
		new Backend().start();
	}

	private void start() throws Exception {
		_unicastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());		
		_unicast = _unicastFactory.newChannel(Stack.newPipeline(this));
		_unicast.bind(new InetSocketAddress(Utils.getWorkableInterface(), 0)).await();
		_unicastAddr = _unicast.getLocalAddress();

		_logger.info("Transport bound on: " + _unicastAddr);
		
		_channels.add(_unicast);		

		_registrar = RegistrarFactory.getRegistrar();
		
		_registrar.register(_unicastAddr);
		
		_paxos = PaxosFactory.init(new ListenerImpl(), CheckpointHandle.NO_CHECKPOINT,
				Utils.marshall(_unicastAddr));				
	}
	
	class ListenerImpl implements Paxos.Listener {
		public void done(VoteOutcome anEvent) {
	        // If we're not the originating node for the post, because we're not leader, 
			// we won't have an address stored up
	        //
	        InetSocketAddress myAddr =
	                (anEvent.getValues().get(HANDBACK_KEY) != null) ? 
	                		_requestMap.remove(new String(anEvent.getValues().get(HANDBACK_KEY))) :
	                        null;

	        if (myAddr == null)
	            return;

	        // Need to translate this into a Backend endpoint using meta-data	        	
	        //
	        try {
	        	InetSocketAddress myBackend = 
	        			Utils.unmarshallInetSocketAddress(_paxos.getMetaData(anEvent.getLeader()));

	        	_unicast.write(new VoteOutcome(anEvent.getResult(), anEvent.getSeqNum(), 
	        			anEvent.getValues(), myBackend), myAddr);
	        } catch (Exception anE) {
	        	_logger.error("Got other leader but can't unmarshall address for backend", anE);
	        }
		}		
	}
	
    public void shutdown() {
		try {
			_channels.close().await();
			
			_logger.debug("Stop unicast factory");
			_unicastFactory.releaseExternalResources();
			
			_paxos.close();
			
			_logger.info("Shutdown complete");
		} catch (Exception anE) {
			_logger.error("Failed to shutdown cleanly", anE);
		}

	}
    
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		_logger.info("Connected: " + ctx + ", " + e);
		_channels.add(e.getChannel());
	}
	
	public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
		_logger.info("Stream open: " + ctx + ", " + e);
		_channels.add(e.getChannel());
	}
	
    public void messageReceived(ChannelHandlerContext aContext, MessageEvent anEvent) {
    	PaxosMessage aMessage = (PaxosMessage) anEvent.getMessage();
    	
		try {
			switch (aMessage.getClassification()) {
				case PaxosMessage.CLIENT : {
                    String myHandback = Long.toString(_handbackGenerator.getAndIncrement());
                    _requestMap.put(myHandback, aMessage.getNodeId());

                    Envelope myEnvelope = (Envelope) aMessage;
                    Proposal myProposal = myEnvelope.getValue();
                    myProposal.put(HANDBACK_KEY, myHandback.getBytes());
                    _paxos.submit(myProposal);
				}

                default : {
                    _logger.debug("Unrecognised message:" + aMessage);
                }
			}
		} catch (Exception anE) {
        	_logger.error("Unexpected exception", anE);
        }
    }

    public void exceptionCaught(ChannelHandlerContext aContext, ExceptionEvent anEvent) {
        _logger.error("Problem in transport", anEvent.getCause());
        // anEvent.getChannel().close();
    }		
}
