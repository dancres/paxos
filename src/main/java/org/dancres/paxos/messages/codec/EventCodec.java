package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Event;
import org.dancres.paxos.messages.Operations;

public class EventCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        /*
         * Event is really a simple adapter around VoteOutcome which contains all the value so encode VoteOutcome only
         */
        Event myEvent = (Event) anObject;
        VoteOutcome myOutcome = myEvent.getOutcome();

        byte[] myBytes = myOutcome.getValues().marshall();
        
        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 4 + 8 + 8 + 4 + 8 + myBytes.length);

        myBuffer.putInt(Operations.EVENT);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myEvent.getSeqNum());
        myBuffer.putLong(myOutcome.getRndNumber());
        myBuffer.putInt(myOutcome.getResult());
        myBuffer.putLong(Codecs.flatten(myOutcome.getLeader()));
        myBuffer.put(myBytes);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();
        
        long mySeqNum = aBuffer.getLong();
        long myRndNum = aBuffer.getLong();
        int myResult = aBuffer.getInt();
        long myNodeId = aBuffer.getLong();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        
        return new Event(new VoteOutcome(myResult, mySeqNum, myRndNum, new Proposal(myBytes),
                Codecs.expand(myNodeId)));
    }
}
