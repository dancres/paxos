package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.messages.Operations;

public class SuccessCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Success mySuccess = (Success) anObject;
        byte[] myBytes = mySuccess.getConsolidatedValue().marshall();

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 4 + 8 + 8 + 8 + myBytes.length);

        myBuffer.putInt(4 + 4 + 8 + 8 + 8 + myBytes.length);
        myBuffer.putInt(Operations.SUCCESS);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(mySuccess.getSeqNum());
        myBuffer.putLong(mySuccess.getRndNum());
        myBuffer.putLong(mySuccess.getNodeId());
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard length
        aBuffer.getInt();

        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        
        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);

        return new Success(mySeqNum, myRndNum, new ConsolidatedValue(myBytes), myNodeId);
    }
}
