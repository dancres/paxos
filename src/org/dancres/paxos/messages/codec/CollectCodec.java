package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Operations;

public class CollectCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Collect myCollect = (Collect) anObject;

        ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.COLLECT);
        myBuffer.putLong(myCollect.getSeqNum());
        myBuffer.putLong(myCollect.getRndNumber());
        myBuffer.putLong(myCollect.getNodeId());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
    	// Discard type
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myRnd = aBuffer.getLong();
        long myNode = aBuffer.getLong();

        return new Collect(mySeq, myRnd, myNode);
    }
}
