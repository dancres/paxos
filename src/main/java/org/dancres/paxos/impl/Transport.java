package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
    public interface Filter {
        /**
         * @param aTransport
         * @param aPacket
         * @return A potentially altered packet upon which processing should continue or null to drop it.
         */
        public Packet filter(Transport aTransport, Packet aPacket);
    }

	public interface Packet {
		public InetSocketAddress getSource();
		public PaxosMessage getMessage();
	}
	
    public interface PacketPickler extends java.io.Serializable {
        public Packet newPacket(PaxosMessage aMessage);
        public byte[] pickle(Packet aPacket);
        public Packet unpickle(byte[] aBytes);
    }

    public FailureDetector getFD();

    public PacketPickler getPickler();

    public void filterRx(Filter aFilter);

    public void filterTx(Filter aFilter);
    
    /**
     * Requests the transport route packets to a dispatcher.
     *
     * @param aDispatcher
     * @throws Exception
     */
    public void routeTo(Dispatcher aDispatcher) throws Exception;

	public InetSocketAddress getLocalAddress();

    public InetSocketAddress getBroadcastAddress();
	
    /**
     * One shot, unreliable send
     *
     * @param aPacket to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(Packet aPacket, InetSocketAddress anAddr);

    public void terminate();

    public interface Lifecycle {
        public void terminate() throws Exception;
    }

    public interface Dispatcher extends Lifecycle {
        public void packetReceived(Packet aPacket);
    }
}
