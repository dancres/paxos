package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.OldRound;

public class OldRoundCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        OldRound myOldRound = (OldRound) anObject;

        // 4-byte length, 4-byte op, 3 * 8 bytes for OldRound
        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 8 + 8 + 8);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(4 + 3 * 8);
        myBuffer.putInt(Operations.OLDROUND);
        myBuffer.putLong(myOldRound.getSeqNum());
        myBuffer.putLong(myOldRound.getNodeId());
        myBuffer.putLong(myOldRound.getLastRound());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myNodeId = aBuffer.getLong();
        long myRnd = aBuffer.getLong();

        return new OldRound(mySeq, myNodeId, myRnd);
    }
}
