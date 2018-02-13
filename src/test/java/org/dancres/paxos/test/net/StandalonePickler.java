package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.netty.PacketImpl;
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
}

