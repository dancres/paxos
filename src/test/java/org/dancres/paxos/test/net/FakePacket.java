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

    public String toString() {
        return "PK [ " + _address + " ] " + _message;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof FakePacket) {
            FakePacket myPacket = (FakePacket) anObject;
            return ((myPacket._address.equals(_address)) && (myPacket._message.equals(_message)));
        }

        return false;
    }

    public int hashCode() {
        return _message.hashCode() ^ _address.hashCode();
    }
}
