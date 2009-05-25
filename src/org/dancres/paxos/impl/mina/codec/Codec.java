package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;

public interface Codec {
    IoBuffer encode(Object anObject);

    Object decode(IoBuffer aBuffer);
}
