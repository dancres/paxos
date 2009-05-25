package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.core.messages.Success;
import org.dancres.paxos.impl.core.messages.Operations;

public class SuccessCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Success mySuccess = (Success) anObject;
        byte[] myBytes = mySuccess.getValue();

        IoBuffer myBuffer;

        myBuffer = IoBuffer.allocate(4 + 4 + 8 + myBytes.length);

        myBuffer.putInt(Operations.SUCCESS);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(mySuccess.getSeqNum());
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);

        return new Success(mySeqNum, myBytes);
    }
}
