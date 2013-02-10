package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.Learned;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class SuccessCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Learned myLearned = (Learned) anObject;

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 8 + 8);

        myBuffer.putInt(Operations.LEARNED);
        myBuffer.putLong(myLearned.getSeqNum());
        myBuffer.putLong(myLearned.getRndNum());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        
        return new Learned(mySeqNum, myRndNum);
    }
}
