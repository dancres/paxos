package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.messages.Ack;
import org.dancres.paxos.impl.messages.Operations;

public class AckCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Ack myAck = (Ack) anObject;

        IoBuffer myBuffer = IoBuffer.allocate(8 + 8);
        myBuffer.putInt(4 + 8);
        myBuffer.putInt(Operations.ACK);
        myBuffer.putLong(myAck.getSeqNum());
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();
        long mySeqNum = aBuffer.getLong();

        return new Ack(mySeqNum);
    }
}
