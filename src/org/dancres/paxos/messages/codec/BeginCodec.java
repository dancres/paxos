package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Begin;

class BeginCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Begin myBegin = (Begin) anObject;

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.BEGIN);
        myBuffer.putLong(myBegin.getSeqNum());
        myBuffer.putLong(myBegin.getRndNumber());
        myBuffer.putLong(myBegin.getNodeId());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();

        return new Begin(mySeqNum, myRndNum, myNodeId);
    }
}
