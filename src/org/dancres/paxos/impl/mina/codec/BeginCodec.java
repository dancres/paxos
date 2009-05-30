package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Begin;

class BeginCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Begin myBegin = (Begin) anObject;

        IoBuffer myBuffer;

        myBuffer = IoBuffer.allocate(4 + 4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.BEGIN);
        myBuffer.putLong(myBegin.getSeqNum());
        myBuffer.putLong(myBegin.getRndNumber());
        myBuffer.putLong(myBegin.getNodeId());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();

        return new Begin(mySeqNum, myRndNum, myNodeId);
    }
}
