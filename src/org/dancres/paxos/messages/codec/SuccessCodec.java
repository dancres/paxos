package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Success;
import org.dancres.paxos.messages.Operations;

public class SuccessCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Success mySuccess = (Success) anObject;
        byte[] myBytes = mySuccess.getValue();

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 8 + myBytes.length);

        myBuffer.putInt(Operations.SUCCESS);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(mySuccess.getSeqNum());
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);

        return new Success(mySeqNum, myBytes);
    }
}
