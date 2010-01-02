package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Operations;

public class CompleteCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Complete myComplete = (Complete) anObject;

        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8);
        myBuffer.putInt(4 + 8);
        myBuffer.putInt(Operations.COMPLETE);
        myBuffer.putLong(myComplete.getSeqNum());
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();

        return new Complete(mySeqNum);
    }
}
