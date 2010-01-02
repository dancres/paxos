package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.Operations;

class FailCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Fail myFail = (Fail) anObject;

        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8 + 4);
        myBuffer.putInt(4 + 8 + 4);
        myBuffer.putInt(Operations.FAIL);
        myBuffer.putLong(myFail.getSeqNum());
        myBuffer.putInt(myFail.getReason());
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();
        int myReason = aBuffer.getInt();

        return new Fail(mySeqNum, myReason);
    }
}
