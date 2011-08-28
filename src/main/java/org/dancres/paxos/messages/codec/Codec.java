package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

public interface Codec {
    ByteBuffer encode(Object anObject);

    Object decode(ByteBuffer aBuffer);
}
