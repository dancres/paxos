package org.dancres.paxos;

import java.nio.ByteBuffer;

public class Operation {
    private byte[] _value;
    private byte[] _handback;

    public Operation(byte[] aValue, byte[] aHandback) {
        _value = aValue;
        _handback = aHandback;
    }

    public byte[] getValue() {
        return _value;
    }

    public byte[] getHandback() {
        return _handback;
    }

    byte[] getConsolidatedValue() {
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + _handback.length + _value.length);
        myBuffer.putInt(_value.length);
        myBuffer.put(_value);
        myBuffer.put(_handback);

        return myBuffer.array();
    }
}
