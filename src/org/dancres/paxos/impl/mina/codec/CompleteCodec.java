package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.messages.Ack;
import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Operations;

public class CompleteCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Complete myComplete = (Complete) anObject;

        IoBuffer myBuffer = IoBuffer.allocate(8 + 8);
        myBuffer.putInt(4 + 8);
        myBuffer.putInt(Operations.COMPLETE);
        myBuffer.putLong(myComplete.getSeqNum());
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();

        return new Complete(mySeqNum);
    }
}
