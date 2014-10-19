package org.dancres.paxos.test.longterm;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.test.net.OrderedMemoryNetwork;

import java.util.concurrent.atomic.AtomicLong;

class PassiveDecider implements Decider {
    private final AtomicLong _packetsTx = new AtomicLong(0);
    private final AtomicLong _packetsRx = new AtomicLong(0);

    public boolean sendUnreliable(OrderedMemoryNetwork.OrderedMemoryTransport aTransport, Transport.Packet aPacket) {
        _packetsTx.incrementAndGet();
        return true;
    }

    public boolean receive(OrderedMemoryNetwork.OrderedMemoryTransport aTransport, Transport.Packet aPacket) {
        _packetsRx.incrementAndGet();
        return true;
    }

    public long getDropCount() {
        return 0;
    }

    public long getRxPacketCount() {
        return _packetsRx.get();
    }

    public long getTxPacketCount() {
        return _packetsTx.get();
    }

    public void settle() {
    }
}

