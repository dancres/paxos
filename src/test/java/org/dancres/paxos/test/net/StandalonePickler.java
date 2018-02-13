package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;

public class StandalonePickler extends Transport.PicklerSkeleton {
    private final InetSocketAddress _source;

    public StandalonePickler(InetSocketAddress aDefaultSource) {
        _source = aDefaultSource;
    }

    public StandalonePickler() {
        _source = null;
    }

    public Transport.Packet newPacket(PaxosMessage aMessage) {
        return new PacketImpl(aMessage, _source);
    }

    public Transport.Packet newPacket(PaxosMessage aMessage, InetSocketAddress anAddress) {
        return new PacketImpl(aMessage, anAddress);
    }

    static class PacketImpl implements Transport.Packet {
        private final PaxosMessage _msg;
        private final InetSocketAddress _source;
        
        PacketImpl(PaxosMessage aMsg, InetSocketAddress aSource) {
            _msg = aMsg;
            _source = aSource;
        }
        
        public InetSocketAddress getSource() {
            return _source;
        }

        public PaxosMessage getMessage() {
            return _msg;
        }

        public String toString() {
            return "PK [ " + _source + " ] " + _msg;
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof PacketImpl) {
                PacketImpl myPacket = (PacketImpl) anObject;
                return ((myPacket._source.equals(_source)) && (myPacket._msg.equals(_msg)));
            }

            return false;
        }

        public int hashCode() {
            return _msg.hashCode() ^ _source.hashCode();
        }
    }
}

