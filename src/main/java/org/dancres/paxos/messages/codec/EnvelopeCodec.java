package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Envelope;

public class EnvelopeCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Envelope myEnvelope = (Envelope) anObject;
        byte[] myBytes = myEnvelope.getValue().marshall();

        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8 + 8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(Operations.ENVELOPE);
        myBuffer.putInt(myBytes.length);
        myBuffer.putLong(myEnvelope.getSeqNum());
        myBuffer.putLong(Codecs.flatten(myEnvelope.getNodeId()));
        myBuffer.put(myBytes);
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        int myArrLength = aBuffer.getInt();

        long mySeqNum = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        
        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        return new Envelope(mySeqNum, new Proposal(myBytes), Codecs.expand(myNodeId));
    }
}
