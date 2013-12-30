package org.dancres.paxos.messages.codec;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.PaxosMessage;

import java.nio.ByteBuffer;

public class EnvelopeCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Envelope myEnvelope = (Envelope) anObject;
        byte[] myBytes = myEnvelope.getValue().marshall();

        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(PaxosMessage.Types.ENVELOPE);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myEnvelope.getSeqNum());
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
        return new Envelope(mySeqNum, new Proposal(myBytes));
    }
}
