package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.messages.Operations;

public class SuccessCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Success mySuccess = (Success) anObject;

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.SUCCESS);
        myBuffer.putLong(mySuccess.getSeqNum());
        myBuffer.putLong(mySuccess.getRndNum());
        myBuffer.putLong(Codecs.flatten(mySuccess.getNodeId()));

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        
        return new Success(mySeqNum, myRndNum, Codecs.expand(myNodeId));
    }
}
