package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.Operations;

public class HeartbeatCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        ByteBuffer myBuffer = ByteBuffer.allocate(16);

        myBuffer.putInt(12);
        myBuffer.putInt(Operations.HEARTBEAT);
        myBuffer.putLong(((Heartbeat) anObject).getNodeId());
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();

        return new Heartbeat(aBuffer.getLong());
    }
}
