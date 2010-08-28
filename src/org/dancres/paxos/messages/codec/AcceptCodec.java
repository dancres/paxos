package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Operations;

public class AcceptCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Accept myAccept = (Accept) anObject;

        // 4-byte length, 4-byte op, 3 * 8 bytes for Accept
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.ACCEPT);
        myBuffer.putLong(myAccept.getSeqNum());
        myBuffer.putLong(myAccept.getRndNumber());
        myBuffer.putLong(Codecs.flatten(myAccept.getNodeId()));

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myRnd = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        
        return new Accept(mySeq, myRnd, Codecs.expand(myNodeId));
    }
}
