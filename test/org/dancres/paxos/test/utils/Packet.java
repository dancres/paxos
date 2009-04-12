package org.dancres.paxos.test.utils;

import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class Packet {
    private Address _source;
    private PaxosMessage _msg;

    public Packet(Address aSource, PaxosMessage aMsg) {
        _source = aSource;
        _msg = aMsg;
    }

    public PaxosMessage getMsg() {
        return _msg;
    }

    Address getSender() {
        return _source;
    }
}
