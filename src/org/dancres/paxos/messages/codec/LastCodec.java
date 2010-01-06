package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Last;

public class LastCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Last myLast = (Last) anObject;
        byte[] myBytes = myLast.getConsolidatedValue().marshall();

        ByteBuffer myBuffer;

        if (myBytes == null)
            myBuffer = ByteBuffer.allocate(8 + 8 + 8 + 8);
        else
            myBuffer = ByteBuffer.allocate(8 + 8 + 8 + 8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        if (myBytes == null)
            myBuffer.putInt(4 + 8 + 8 + 8);
        else
            myBuffer.putInt(4 + 8 + 8 + 8 + myBytes.length);

        myBuffer.putInt(Operations.LAST);
        myBuffer.putLong(myLast.getSeqNum());
        myBuffer.putLong(myLast.getLowWatermark());
        myBuffer.putLong(myLast.getRndNumber());

        if (myBytes != null)
            myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        int myArrLength = aBuffer.getInt() - (4 + 8 + 8 + 8);

        // Discard type
        aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myLow = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        
		byte[] myBytes = new byte[myArrLength];
		aBuffer.get(myBytes);

		return new Last(mySeqNum, myLow, myRndNum, new ConsolidatedValue(myBytes));
    }
}
