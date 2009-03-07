package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.core.messages.Collect;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.NetworkUtils;

public class CollectCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Collect myCollect = (Collect) anObject;

        IoBuffer myBuffer = IoBuffer.allocate(4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.COLLECT);
        myBuffer.putLong(myCollect.getSeqNum());
        myBuffer.putLong(myCollect.getRndNumber());
        myBuffer.putLong(myCollect.getNodeId());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
    	// Discard type
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myRnd = aBuffer.getLong();
        long myNode = aBuffer.getLong();

        return new Collect(mySeq, myRnd, myNode);
    }
}
