package org.dancres.paxos.impl.mina.io;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mina.common.ConnectFuture;
import org.apache.mina.common.IoSession;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.dancres.paxos.Transport;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class TransportImpl implements Transport {

    private ConcurrentHashMap<NodeId, IoSession> _sessions = new ConcurrentHashMap<NodeId, IoSession>();
    
    /**
     * The address where our unicast port is bound. We add a note of this to outgoing proposer messages so that
     * where appropriate we can be called back.
     */
    private InetSocketAddress _addr;
    
    private NioDatagramConnector _unicastConnector;

    /**
     * @param aNodeAddr is the address of our unicast port
     * @param aBroadcastSession is the session on which to send broadcast messages for acceptor learners
     * @param aUnicastConnector is the connector to use for contacting hosts directly when specified via NodeId
     */
    public TransportImpl(InetSocketAddress aNodeAddr, IoSession aBroadcastSession, 
    		NioDatagramConnector aUnicastConnector) {
        super();
        _sessions.put(NodeId.BROADCAST, aBroadcastSession);
        _addr = aNodeAddr;
        _unicastConnector = aUnicastConnector;
    }

    public void send(PaxosMessage aMessage, NodeId anAddress) {
        PaxosMessage myMessage;

        IoSession mySession = (IoSession) _sessions.get(anAddress);
            
		if (mySession == null) {
			try {
				ConnectFuture connFuture = _unicastConnector.connect(NodeId
						.toAddress(anAddress));
				connFuture.awaitUninterruptibly();
				IoSession myNewSession = connFuture.getSession();
				
				mySession = _sessions.putIfAbsent(anAddress, myNewSession);
				
				/*
				 * We're racing to create the session, if we were beaten to it, mySession will be non-null
				 * and we must close the new session we just created. Otherwise, we're good to go with myNewSession
				 */
				if (mySession == null)
					mySession = myNewSession;
				else {
					myNewSession.close();
				}
			} catch (Exception anE) {
				throw new RuntimeException("Got connection problem", anE);
			}
		}
		
        mySession.write(aMessage);
    }
}
