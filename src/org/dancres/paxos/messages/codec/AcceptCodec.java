package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Operations;

public class AcceptCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Accept myAccept = (Accept) anObject;

        // 4-byte length, 4-byte op, 2 * 8 bytes for Accept
        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8 + 8);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(4 + 2 * 8);
        myBuffer.putInt(Operations.ACCEPT);
        myBuffer.putLong(myAccept.getSeqNum());
        myBuffer.putLong(myAccept.getRndNumber());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myRnd = aBuffer.getLong();

        return new Accept(mySeq, myRnd);
    }
}
