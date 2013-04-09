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
	
    public interface PacketPickler extends java.io.Serializable {
        public byte[] pickle(Packet aPacket);
        public Packet unpickle(byte[] aBytes);
    }

    public PacketPickler getPickler();

    public void routeTo(Dispatcher aDispatcher) throws Exception;

	public InetSocketAddress getLocalAddress();

    public InetSocketAddress getBroadcastAddress();
	
    /**
     * @param aMessage is the message to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, InetSocketAddress anAddr);

    public void connectTo(InetSocketAddress anAddr, ConnectionHandler aHandler);

    public interface Stream {
        public void close();
        public void send(PaxosMessage aMessage);
        public void sendRaw(Transport.Packet aPacket);
    }

    public interface ConnectionHandler {
        public void connected(Stream aStream);
    }

    public void terminate();

    public interface Lifecycle {
        /**
         * @param aTransport
         * @throws Exception
         */
        public void init(Transport aTransport) throws Exception;
        public void terminate() throws Exception;
    }

    public interface Dispatcher extends Lifecycle {
        /**
         * @return <code>true</code> to indicate that this packet has been processed and should not be given to
         * other handlers.
         */
        public boolean messageReceived(Packet aPacket);
    }
}
