package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

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
        Packet newPacket(PaxosMessage aMessage, InetSocketAddress anAddress);
        byte[] pickle(Packet aPacket);
        Packet unpickle(byte[] aBytes);
    }

    abstract class PicklerSkeleton implements PacketPickler {
        public byte[] pickle(Transport.Packet aPacket) {
            byte[] myBytes = Codecs.encode(aPacket.getMessage());
            ByteBuffer myBuffer = ByteBuffer.allocate(8 + 4 + myBytes.length);

            myBuffer.putLong(Codecs.flatten(aPacket.getSource()));
            myBuffer.putInt(myBytes.length);
            myBuffer.put(myBytes);
            myBuffer.flip();

            return myBuffer.array();
        }

        public Transport.Packet unpickle(byte[] aBytes) {
            ByteBuffer myBuffer = ByteBuffer.wrap(aBytes);

            InetSocketAddress mySource = Codecs.expand(myBuffer.getLong());
            int myLength = myBuffer.getInt();
            byte[] myPaxosBytes = new byte[myLength];
            myBuffer.get(myPaxosBytes);

            PaxosMessage myMessage = Codecs.decode(myPaxosBytes);

            return newPacket(myMessage, mySource);
        }
    }

    FailureDetector getFD();

    PacketPickler getPickler();

    void filterRx(Filter aFilter);

    void filterTx(Filter aFilter);
    
    /**
     * Requests the transport route packets to a dispatcher.
     *
     * @param aDispatcher
     */
    void routeTo(Dispatcher aDispatcher);

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
