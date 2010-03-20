package org.dancres.paxos.impl.mina.io;

import org.dancres.paxos.impl.*;
import org.apache.mina.transport.socket.DatagramAcceptor;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.DatagramConnector;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.mina.filter.logging.LogLevel;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IoEventType;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.mina.codec.PaxosCodecFactory;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.Leader;
import org.dancres.paxos.LivenessListener;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.InetSocketAddress;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.NodeId;

public class PaxosPeer {
    private static Logger _logger = LoggerFactory.getLogger(PaxosPeer.class);
    
    public static final int BROADCAST_PORT = 41952;

    public static void main(String anArgs[]) throws Exception {
    	LoggingFilter myFilter = new LoggingFilter();

    	IoEventType[] myTypes = IoEventType.values(); 
    	for (int i = 0; i < myTypes.length; i++) {
    		myFilter.setLogLevel(myTypes[i], LogLevel.DEBUG);
    	}
    	
        /*
         * Setup the stacks for Acceptor/Learner
         *
         * Send unicast messages to Proposer
         * Receive broadcasts from Proposer
         *   - Failure detector uses broadcast messages to ascertain liveness
         * 
         */
    	PaxosPacketHandler myHandler = new PaxosPacketHandler();
    	
    	/*
    	 * Unicast sender channel, used by AcceptorLearners to talk directly with Leader.
    	 */
        NioDatagramConnector myUnicastSender = new NioDatagramConnector();
        myUnicastSender.getFilterChain().addLast( "logger", myFilter);
        myUnicastSender.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));
        myUnicastSender.setHandler(myHandler);

        /*
         * Unicast receiver used by Leader to receive direct messages from AcceptorLearners
         */
        InetSocketAddress myAddr = new InetSocketAddress(NetworkUtils.getWorkableInterface(), 0);

        DatagramAcceptor myUnicastChannel = new NioDatagramAcceptor();
        myUnicastChannel.setHandler(myHandler);
        myUnicastChannel.getFilterChain().addLast( "logger", myFilter);
        myUnicastChannel.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));
        myUnicastChannel.bind(myAddr);
        _logger.info("PaxosPeer bound on port: " + myUnicastChannel.getLocalAddress());
        
        /*
         * Receives broadcast messages, used by AcceptorLearners to receive messages from Leader. 
         */
        DatagramAcceptor myPropBcast = new NioDatagramAcceptor();
        myPropBcast.setHandler(myHandler);
        myPropBcast.getFilterChain().addLast( "logger", myFilter);
        myPropBcast.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));

        DatagramSessionConfig myDcfg = myPropBcast.getSessionConfig();
        myDcfg.setReuseAddress(true);
        myPropBcast.bind(new InetSocketAddress(BROADCAST_PORT));

        /*
         * Broadcast channel used by leader to send messages to all AcceptorLearners
         */
        DatagramConnector myBroadcastChannel = new NioDatagramConnector();
        myBroadcastChannel.getSessionConfig().setBroadcast(true);
        myBroadcastChannel.getSessionConfig().setReuseAddress(true);
        myBroadcastChannel.setHandler(myHandler);
        myBroadcastChannel.getFilterChain().addLast( "logger", myFilter);
        myBroadcastChannel.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));

        ConnectFuture myConnFuture =
                myBroadcastChannel.connect(new InetSocketAddress(NetworkUtils.getBroadcastAddress(), BROADCAST_PORT));
        myConnFuture.awaitUninterruptibly();
        IoSession myBroadcastSession = myConnFuture.getSession();

        _logger.info("Broadcasting on: " + NetworkUtils.getBroadcastAddress());
        _logger.info("Broadcasting from: " + myBroadcastSession.getLocalAddress());

        TransportImpl myTransport = new TransportImpl(myBroadcastSession, myUnicastSender);
        
        FailureDetectorImpl myFd = new FailureDetectorImpl(5000);
        myFd.add(new ListenerImpl());

        AcceptorLearner myAl = new AcceptorLearner(new MemoryLogStorage(), myFd, myTransport, 
        		NodeId.from(myUnicastChannel.getLocalAddress()));

        Leader myLeader = new Leader(myFd, NodeId.from(myUnicastChannel.getLocalAddress()), myTransport, myAl);

        myHandler.setAcceptorLearner(myAl);
        myHandler.setFailureDetector(myFd);
        myHandler.setLeader(myLeader);
        
        Thread myHeartbeater = new Thread(
        		new Heartbeater(NodeId.from(myUnicastChannel.getLocalAddress()), myTransport));
        myHeartbeater.setDaemon(true);
        myHeartbeater.start();
        
        /*
        _logger.info("Paxos Name: " + flatten(myClientUnicast.getLocalAddress().getAddress()));
        
        JmDNS myAdvertiser = JmDNS.create(myClientUnicast.getLocalAddress().getAddress(),
        		flatten(myClientUnicast.getLocalAddress().getAddress()));
        
        ServiceInfo myInfo = ServiceInfo.create("_paxos._udp.local", "paxos", 
        	myClientUnicast.getLocalAddress().getPort(), "");
        
        myAdvertiser.registerService(myInfo);
        */

        /*
        Registrar myReg = RegistrarFactory.getRegistrar();
        myReg.register(myClientUnicast.getLocalAddress());
        
        Thread.sleep(10000);
        
        HostDetails[] myHosts = myReg.find(10000);
        
        _logger.info("Found " + myHosts.length + " paxos services");
        
        for (int i = 0; i < myHosts.length; i++) {
        	_logger.info("Host: " + myHosts[i]); 
        }
        */
        
    }

    static class ListenerImpl implements LivenessListener {
        private Logger _logger;

        ListenerImpl() {
            _logger = LoggerFactory.getLogger(getClass());
        }

        public void alive(NodeId aProcess) {
            _logger.info("**********Alive************ " + aProcess);
        }

        public void dead(NodeId aProcess) {
            _logger.info("!!!!!!!!!!Dead!!!!!!!!!!! " + aProcess);
        }
    }
}
