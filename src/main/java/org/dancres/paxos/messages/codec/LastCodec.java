package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Last;

public class LastCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Last myLast = (Last) anObject;
        byte[] myBytes = myLast.getConsolidatedValue().marshall();

        ByteBuffer myBuffer;

        if (myBytes == null)
            myBuffer = ByteBuffer.allocate(8 + 8 + 8 + 8 + 8);
        else
            myBuffer = ByteBuffer.allocate(8 + 8 + 8 + 8 + 8 + myBytes.length);

        myBuffer.putInt(Operations.LAST);

        if (myBytes == null)
            myBuffer.putInt(0);
        else
            myBuffer.putInt(myBytes.length);

        myBuffer.putLong(myLast.getSeqNum());
        myBuffer.putLong(myLast.getLowWatermark());
        myBuffer.putLong(myLast.getRndNumber());
        myBuffer.putLong(Codecs.flatten(myLast.getNodeId()));
        
        if (myBytes != null)
            myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        // Discard the length and operation so remaining data can be processed
        // separately
        int myArrLength = aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myLow = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        
		byte[] myBytes = new byte[myArrLength];
		aBuffer.get(myBytes);

		return new Last(mySeqNum, myLow, myRndNum, new Proposal(myBytes), Codecs.expand(myNodeId));
    }
}
