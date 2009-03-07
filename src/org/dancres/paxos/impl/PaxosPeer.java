package org.dancres.paxos.impl;

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
import org.dancres.paxos.impl.io.mina.AcceptorLearnerAdapter;
import org.dancres.paxos.impl.codec.PaxosCodecFactory;
import org.dancres.paxos.impl.discovery.HostDetails;
import org.dancres.paxos.impl.discovery.Registrar;
import org.dancres.paxos.impl.discovery.RegistrarFactory;
import org.dancres.paxos.impl.io.mina.ProposerAdapter;
import org.dancres.paxos.impl.io.mina.FailureDetectorAdapter;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.faildet.LivenessListener;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;
import org.dancres.paxos.impl.io.mina.ChannelImpl;

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
        FailureDetectorAdapter myDetectorAdapter = new FailureDetectorAdapter();
        myDetectorAdapter.add(new ListenerImpl());

        DatagramConnector myPropUnicast = new NioDatagramConnector();
        myPropUnicast.getFilterChain().addLast( "logger", myFilter);
        myPropUnicast.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));

        AcceptorLearnerAdapter myAccLearn = new AcceptorLearnerAdapter(myPropUnicast);
        myPropUnicast.setHandler(myAccLearn);
        DatagramAcceptor myPropBcast = new NioDatagramAcceptor();
        myPropBcast.setHandler(myAccLearn);
        myPropBcast.getFilterChain().addLast( "logger", myFilter);
        myPropBcast.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));
        myPropBcast.getFilterChain().addLast("failureDetector", myDetectorAdapter);

        DatagramSessionConfig myDcfg = myPropBcast.getSessionConfig();
        myDcfg.setReuseAddress(true);
        myPropBcast.bind(new InetSocketAddress(BROADCAST_PORT));

        /*
         * Setup the stacks for Proposer
         *
         * Sends broadcast messages to all participants
         * Receives unitcast messages from clients
         *   - Heartbeat messages are sent on the broadcast channel to drive the failure detector
         *
         */
        InetSocketAddress myAddr = new InetSocketAddress(NetworkUtils.getWorkableInterface(), 0);

        ProposerAdapter myProposer = new ProposerAdapter();
        DatagramConnector myPartBroad = new NioDatagramConnector();
        myPartBroad.getSessionConfig().setBroadcast(true);
        myPartBroad.getSessionConfig().setReuseAddress(true);        
        myPartBroad.setHandler(myProposer);
        myPartBroad.getFilterChain().addLast( "logger", myFilter);
        myPartBroad.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));

        ConnectFuture connFuture =
                myPartBroad.connect(new InetSocketAddress(NetworkUtils.getBroadcastAddress(), BROADCAST_PORT));
        connFuture.awaitUninterruptibly();
        IoSession mySession = connFuture.getSession();

        _logger.info("Broadcasting on: " + NetworkUtils.getBroadcastAddress());
        
        DatagramAcceptor myClientUnicast = new NioDatagramAcceptor();
        myClientUnicast.setHandler(myProposer);
        myClientUnicast.getFilterChain().addLast( "logger", myFilter);
        myClientUnicast.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));
        myClientUnicast.bind(myAddr);
        _logger.info("PaxosPeer bound on port: " + myClientUnicast.getLocalAddress());

        myProposer.init(mySession, myDetectorAdapter.getDetector(), myClientUnicast.getLocalAddress());

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
        
        Thread myHeartbeater = new Thread(new Heartbeater(new ChannelImpl(mySession)));
        myHeartbeater.setDaemon(true);
        myHeartbeater.start();
    }

    static class ListenerImpl implements LivenessListener {
        private Logger _logger;

        ListenerImpl() {
            _logger = LoggerFactory.getLogger(getClass());
        }

        public void alive(SocketAddress aProcess) {
            _logger.info("**********Alive************ " + aProcess);
        }

        public void dead(SocketAddress aProcess) {
            _logger.info("!!!!!!!!!!Dead!!!!!!!!!!! " + aProcess);
        }
    }
}
