package org.dancres.paxos;

public class Operation {
    private byte[] _value;

    public Operation(byte[] aValue) {
        _value = aValue;
    }

    public byte[] getValue() {
        return _value;
    }
}
