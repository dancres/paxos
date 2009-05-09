package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.Address;

public class Motion implements PaxosMessage {
    private long _seqNum;
    private byte[] _value;
    private Address _clientAddress;

    public Motion(long aSeqNum, byte[] aValue) {
        _seqNum = aSeqNum;
        _value = aValue;
    }

    public int getType() {
        return Operations.MOTION;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public byte[] getValue() {
        return _value;
    }
}
