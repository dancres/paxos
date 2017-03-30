package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
    interface Filter {
        /**
         * @param aTransport
         * @param aPacket
         * @return A potentially altered packet upon which processing should continue or null to drop it.
         */
        Packet filter(Transport aTransport, Packet aPacket);
    }

	interface Packet {
		InetSocketAddress getSource();
		PaxosMessage getMessage();
	}
	
    interface PacketPickler extends java.io.Serializable {
        Packet newPacket(PaxosMessage aMessage);
        byte[] pickle(Packet aPacket);
        Packet unpickle(byte[] aBytes);
    }

    FailureDetector getFD();

    PacketPickler getPickler();

    void filterRx(Filter aFilter);

    void filterTx(Filter aFilter);
    
    /**
     * Requests the transport route packets to a dispatcher.
     *
     * @param aDispatcher
     * @throws Exception
     */
    void routeTo(Dispatcher aDispatcher) throws Exception;

	InetSocketAddress getLocalAddress();

    InetSocketAddress getBroadcastAddress();
	
    /**
     * One shot, unreliable send
     *
     * @param aPacket to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    void send(Packet aPacket, InetSocketAddress anAddr);

    void terminate();

    interface Lifecycle {
        void terminate() throws Exception;
    }

    interface Dispatcher extends Lifecycle {
        void packetReceived(Packet aPacket);
    }
}
