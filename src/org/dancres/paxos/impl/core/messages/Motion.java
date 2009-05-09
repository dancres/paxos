package org.dancres.paxos.impl.core.messages;

public class Motion implements PaxosMessage {
    private byte[] _value;

    public Motion(byte[] aValue) {
        _value = aValue;
    }

    public int getType() {
        return Operations.MOTION;
    }

    public long getSeqNum() {
        throw new UnsupportedOperationException();
    }

    public byte[] getValue() {
        return _value;
    }
}
