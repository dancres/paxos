package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.Last;

public class LastCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Last myLast = (Last) anObject;
        byte[] myBytes = myLast.getValue();

        IoBuffer myBuffer;

        if (myBytes == null)
            myBuffer = IoBuffer.allocate(8 + 8 + 8);
        else
            myBuffer = IoBuffer.allocate(8 + 8 + 8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        if (myBytes == null)
            myBuffer.putInt(4 + 8 + 8);
        else
            myBuffer.putInt(4 + 8 + 8 + myBytes.length);

        myBuffer.putInt(Operations.LAST);
        myBuffer.putLong(myLast.getSeqNum());
        myBuffer.putLong(myLast.getRndNumber());

        if (myBytes != null)
            myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        int myArrLength = aBuffer.getInt() - (4 + 8 + 8);

        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        
        if (myArrLength != 0) {
            byte[] myBytes = new byte[myArrLength];
            aBuffer.get(myBytes);

            return new Last(mySeqNum, myRndNum, myBytes);
        } else
            return new Last(mySeqNum, myRndNum, null);
    }
}
