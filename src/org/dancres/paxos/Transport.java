package org.dancres.paxos;

import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
	public InetSocketAddress getLocalAddress();

    public InetSocketAddress getBroadcastAddress();
	
    /**
     * @param aMessage is the message to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, InetSocketAddress anAddr);

    public Stream connectTo(InetSocketAddress anAddr);
    
    public void shutdown();

    public interface Dispatcher {
        public void setTransport(Transport aTransport) throws Exception;
        public void messageReceived(PaxosMessage aMessage);
    }
}
