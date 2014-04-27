package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

interface Codec {
    ByteBuffer encode(Object anObject);

    Object decode(ByteBuffer aBuffer);
}
