package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.OldRound;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class OldRoundCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        OldRound myOldRound = (OldRound) anObject;

        // 4-byte length, 4-byte op, 4 * 8 bytes for OldRound
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8 + 8 + 8);

        myBuffer.putInt(Operations.OLDROUND);
        myBuffer.putLong(myOldRound.getSeqNum());
        myBuffer.putLong(Codecs.flatten(myOldRound.getLeaderNodeId()));
        myBuffer.putLong(myOldRound.getLastRound());

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        long mySeq = aBuffer.getLong();
        long myLeaderNodeId = aBuffer.getLong();
        long myRnd = aBuffer.getLong();
        
        return new OldRound(mySeq, Codecs.expand(myLeaderNodeId), myRnd);
    }
}
