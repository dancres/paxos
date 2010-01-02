package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.faildet.Heartbeat;

public class HeartbeatCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        ByteBuffer myBuffer = ByteBuffer.allocate(8);

        myBuffer.putInt(4);
        myBuffer.putInt(Heartbeat.TYPE);
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();

        return new Heartbeat();
    }
}
