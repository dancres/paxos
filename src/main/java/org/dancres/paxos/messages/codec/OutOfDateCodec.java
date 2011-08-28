package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.OutOfDate;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class OutOfDateCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        OutOfDate myNeed = (OutOfDate) anObject;

        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4 + 8 + 8 + 8);
        myBuffer.putInt(Operations.OUTOFDATE);
        myBuffer.putLong(Codecs.flatten(myNeed.getNodeId()));

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long myNodeId = aBuffer.getLong();

        return new OutOfDate(Codecs.expand(myNodeId));
    }
}
