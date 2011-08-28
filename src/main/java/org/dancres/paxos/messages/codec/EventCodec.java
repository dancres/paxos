package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Operations;

public class EventCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Event myEvent = (Event) anObject;
        byte[] myBytes = myEvent.getValues().marshall();
        
        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 8 + 4 + 8 + myBytes.length);

        myBuffer.putInt(Operations.EVENT);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myEvent.getSeqNum());
        myBuffer.putInt(myEvent.getResult());
        myBuffer.putLong(Codecs.flatten(myEvent.getNodeId()));
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        
        long mySeqNum = aBuffer.getLong();
        int myResult = aBuffer.getInt();
        long myNodeId = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        
        return new Event(myResult, mySeqNum, new Proposal(myBytes), Codecs.expand(myNodeId));
    }
}
