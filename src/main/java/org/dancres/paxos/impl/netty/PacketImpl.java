package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

class PacketImpl implements Transport.Packet {
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
}

