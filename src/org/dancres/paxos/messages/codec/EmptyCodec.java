package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

public class EmptyCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        throw new RuntimeException("Undefined codec: " + anObject);
    }

    public Object decode(ByteBuffer aBuffer) {
        throw new RuntimeException("Undefined codec: " + aBuffer.getInt(4));
    }
}
