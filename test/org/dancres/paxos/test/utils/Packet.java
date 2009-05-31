package org.dancres.paxos.test.utils;

import org.dancres.paxos.NodeId;
import org.dancres.paxos.messages.PaxosMessage;

public class Packet {
    private NodeId _source;
    private PaxosMessage _msg;

    public Packet(NodeId aSource, PaxosMessage aMsg) {
        _source = aSource;
        _msg = aMsg;
    }

    public PaxosMessage getMsg() {
        return _msg;
    }

    public NodeId getSender() {
        return _source;
    }
}
