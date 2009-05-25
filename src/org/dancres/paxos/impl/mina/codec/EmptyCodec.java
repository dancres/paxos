package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;

public class EmptyCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        throw new RuntimeException("Undefined codec: " + anObject);
    }

    public Object decode(IoBuffer aBuffer) {
        throw new RuntimeException("Undefined codec: " + aBuffer.getInt(4));
    }
}
