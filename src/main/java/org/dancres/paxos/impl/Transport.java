package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
	public interface Packet {
		public InetSocketAddress getSource();
		public PaxosMessage getMessage();
	}
	
    public interface PacketPickler {
        public byte[] pickle(Packet aPacket);
        public Packet unpickle(byte[] aBytes);
    }

    public PacketPickler getPickler();

    public void add(Dispatcher aDispatcher) throws Exception;

	public InetSocketAddress getLocalAddress();

    public InetSocketAddress getBroadcastAddress();
	
    /**
     * @param aMessage is the message to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, InetSocketAddress anAddr);

    public void connectTo(InetSocketAddress anAddr, ConnectionHandler aHandler);

    public interface ConnectionHandler {
        public void connected(Stream aStream);
    }

    public void shutdown();

    public interface Dispatcher {
        public void setTransport(Transport aTransport) throws Exception;

        /**
         * @param aMessage
         * @return <code>true</code> to indicate that this packet has been processed and should not be given to
         * other handlers.
         */
        public boolean messageReceived(Packet aPacket);
    }
}
