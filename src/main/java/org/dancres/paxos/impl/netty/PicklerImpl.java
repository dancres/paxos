package org.dancres.paxos.impl.netty;

import java.net.InetSocketAddress;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;

public class PicklerImpl extends Transport.PicklerSkeleton {
    private final InetSocketAddress _source;

    public PicklerImpl(InetSocketAddress aDefaultSource) {
        _source = aDefaultSource;
    }

    public PicklerImpl() {
        _source = null;
    }

    public Transport.Packet newPacket(PaxosMessage aMessage) {
        return new PacketImpl(aMessage, _source);
    }

    public Transport.Packet newPacket(PaxosMessage aMessage, InetSocketAddress anAddress) {
        return new PacketImpl(aMessage, anAddress);
    }
}

