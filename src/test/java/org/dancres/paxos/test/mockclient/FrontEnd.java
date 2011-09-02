package org.dancres.paxos.test.mockclient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;

import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.discovery.HostDetails;
import org.dancres.paxos.impl.discovery.Registrar;
import org.dancres.paxos.impl.discovery.RegistrarFactory;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChildChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontEnd extends SimpleChannelHandler {
	private static Logger _logger = LoggerFactory
			.getLogger(FrontEnd.class);
	
	private InetSocketAddress _unicastAddr;
	private DatagramChannelFactory _unicastFactory;
	private DatagramChannel _unicast;
    private ChannelGroup _channels = new DefaultChannelGroup();
    private Registrar _registrar;
	private List<Event> _queue = new ArrayList<Event>();    
	private InetSocketAddress _leaderAddr = null;
	private Random _random = new Random();
	
	public static void main(String[] anArgs) throws Exception {
		FrontEnd mine = new FrontEnd();
		mine.start();

		boolean exit = false;
		
		Proposal myProp = new Proposal("rhubarb", "custard".getBytes());
		
		while (! exit) {
			_logger.info("Sending request");
			
			mine.submit(myProp);

			Event myResp = mine.getNext(10000);

			if (myResp == null)
				throw new RuntimeException("No response :(");

			switch (myResp.getResult()) {
				case Event.Reason.DECISION : {
					System.out.println("Wooooo!");
					exit = true;
					break;
				}

				case Event.Reason.OTHER_LEADER : {
					System.out.println("Try another leader:" + myResp.getLeader());
					mine.newLeader(myResp.getLeader());
					break;
				}

				default : {
					System.out.println("Not cool!");
					exit = true;
					break;
				}
			}		
		}
	}
	
	private void start() throws Exception {
		_unicastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool());		
		_unicast = _unicastFactory.newChannel(Stack.newPipeline(this));
		_unicast.bind(new InetSocketAddress(Utils.getWorkableInterface(), 0)).await();
		_unicastAddr = _unicast.getLocalAddress();

		_logger.info("Transport bound on: " + _unicastAddr);
		
		_channels.add(_unicast);		

		_registrar = RegistrarFactory.getRegistrar();
		
		HostDetails[] myHosts = _registrar.find(10000);
		
		if ((myHosts == null) || (myHosts.length == 0))
			throw new RuntimeException("Didn't find any paxos nodes :(");
		else {
			int myChoice = _random.nextInt(myHosts.length);
			_logger.info("Decided leader is: " + myHosts[myChoice].getAddr());
			_leaderAddr = myHosts[myChoice].getAddr();
		}
	}	
	
    public void exceptionCaught(ChannelHandlerContext aContext, ExceptionEvent anEvent) {
        _logger.error("Problem in transport", anEvent.getCause());
        // anEvent.getChannel().close();
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
    	PaxosMessage myMessage = (PaxosMessage) anEvent.getMessage();
    	
		synchronized(this) {
	        switch (myMessage.getType()) {
	        	case Operations.EVENT : {
	        		_queue.add((Event) myMessage);
	        		notifyAll();
	        		break;
	        	}
            }
        }
    }
    
    public void newLeader(InetSocketAddress aLeader) {
    	_leaderAddr = aLeader;
    }
    
    public void submit(Proposal aProposal) {
    	_logger.info("Sending message from: " + _unicastAddr + " to " + _leaderAddr);
    	
    	_unicast.write(new Envelope(aProposal, _unicastAddr), _leaderAddr);
    }
    
	public Event getNext(long aTimeout) {
		synchronized(this) {
			while (_queue.isEmpty()) {
				try {
					wait(aTimeout);
				} catch (InterruptedException anIE) {					
				}
			}
			
			return _queue.remove(0);
		}
	}    
}
