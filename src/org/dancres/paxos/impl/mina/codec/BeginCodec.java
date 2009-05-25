package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Begin;

class BeginCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Begin myBegin = (Begin) anObject;
        byte[] myBytes = myBegin.getValue();

        IoBuffer myBuffer;

        myBuffer = IoBuffer.allocate(4 + 4 + 8 + 8 + 8 + myBytes.length);

        myBuffer.putInt(Operations.BEGIN);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myBegin.getSeqNum());
        myBuffer.putLong(myBegin.getRndNumber());
        myBuffer.putLong(myBegin.getNodeId());
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        // Get length of byte array
        int myArrLength = aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);

        return new Begin(mySeqNum, myRndNum, myNodeId, myBytes);
    }
}
