package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.messages.Heartbeat;
import org.dancres.paxos.impl.messages.Operations;
import org.dancres.paxos.impl.NetworkUtils;

public class HeartbeatCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        IoBuffer myBuffer = IoBuffer.allocate(8);

        myBuffer.putInt(4);
        myBuffer.putInt(Operations.HEARTBEAT);
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
