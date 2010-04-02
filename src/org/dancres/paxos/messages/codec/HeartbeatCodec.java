package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.Operations;

public class HeartbeatCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        ByteBuffer myBuffer = ByteBuffer.allocate(12);

        myBuffer.putInt(Operations.HEARTBEAT);
        myBuffer.putLong(((Heartbeat) anObject).getNodeId());
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        return new Heartbeat(aBuffer.getLong());
    }
}
