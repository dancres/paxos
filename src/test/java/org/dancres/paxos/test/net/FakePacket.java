package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.Transport;

public class FakePacket implements Transport.Packet {
    private PaxosMessage _message;
    private InetSocketAddress _address;

    public FakePacket(PaxosMessage aMessage) {
        _message = aMessage;

        try {
            _address = new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 12345);
        } catch (Exception anE) {
            throw new RuntimeException("No localhost address, doomed", anE);
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
