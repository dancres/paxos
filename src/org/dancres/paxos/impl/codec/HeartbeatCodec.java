package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.faildet.Heartbeat;

public class HeartbeatCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        IoBuffer myBuffer = IoBuffer.allocate(8);

        myBuffer.putInt(4);
        myBuffer.putInt(Heartbeat.TYPE);
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        aBuffer.getInt();
        aBuffer.getInt();

        return new Heartbeat();
    }
}
