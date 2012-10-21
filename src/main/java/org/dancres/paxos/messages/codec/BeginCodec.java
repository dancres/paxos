package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Begin;

/**
 * @todo Optimise a la LastCodec, we don't need that length integer as we can deduce it from the size of the buffer
 */
class BeginCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Begin myBegin = (Begin) anObject;
        byte[] myBytes = myBegin.getConsolidatedValue().marshall();
        
        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 8 + 8 + myBytes.length);

        myBuffer.putInt(Operations.BEGIN);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myBegin.getSeqNum());
        myBuffer.putLong(myBegin.getRndNumber());
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        
        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        
        return new Begin(mySeqNum, myRndNum, new Proposal(myBytes));
    }
}
