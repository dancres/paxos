package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Success;
import org.dancres.paxos.messages.Operations;

public class SuccessCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Success mySuccess = (Success) anObject;

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 8 + 8);

        myBuffer.putInt(Operations.LEARNED);
        myBuffer.putLong(mySuccess.getSeqNum());
        myBuffer.putLong(mySuccess.getRndNum());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        
        return new Success(mySeqNum, myRndNum);
    }
}
