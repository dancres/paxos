package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class CollectCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Collect myCollect = (Collect) anObject;

        ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8 + 8);

        myBuffer.putInt(Operations.COLLECT);
        myBuffer.putLong(myCollect.getSeqNum());
        myBuffer.putLong(myCollect.getRndNumber());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard type
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myRnd = aBuffer.getLong();

        return new Collect(mySeq, myRnd);
    }
}
