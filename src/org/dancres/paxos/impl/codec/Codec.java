package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;

public interface Codec {
    IoBuffer encode(Object anObject);

    Object decode(IoBuffer aBuffer);
}
