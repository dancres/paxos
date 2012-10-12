package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.Transport;

public class FakePacket implements Transport.Packet {
    private PaxosMessage _message;
    private InetSocketAddress _address;

    public FakePacket(PaxosMessage aMessage) {
        _message = aMessage;

        try {
            _address = new InetSocketAddress(InetAddress.getLocalHost(), 12345);
        } catch (Exception anE) {
            throw new RuntimeException("No localhost address, doomed");
        }
    }

    public FakePacket(InetSocketAddress anAddr, PaxosMessage aMessage) {
        _address = anAddr;
        _message = aMessage;
    }

    public InetSocketAddress getSource() {
        return _address;
    }

    public PaxosMessage getMessage() {
        return _message;
    }
}
