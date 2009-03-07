package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class Packet {
    private InetSocketAddress _source;
    private PaxosMessage _msg;

    public Packet(InetSocketAddress aSource, PaxosMessage aMsg) {
        _source = aSource;
        _msg = aMsg;
    }

    public PaxosMessage getMsg() {
        return _msg;
    }

    InetSocketAddress getSender() {
        return _source;
    }
}
