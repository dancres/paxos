package org.dancres.paxos.messages.codec;

import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class HeartbeatCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Heartbeat myHB = (Heartbeat) anObject;
        int myMetaDataLength = myHB.getMetaData().length;
        ByteBuffer myBuffer = ByteBuffer.allocate(8 + myMetaDataLength);

        myBuffer.putInt(Operations.HEARTBEAT);
        myBuffer.putInt(myMetaDataLength);
        myBuffer.put(myHB.getMetaData());
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        int myMetaSize = aBuffer.getInt();

        byte[] myBytes = new byte[myMetaSize];
        aBuffer.get(myBytes);

        return new Heartbeat(myBytes);
    }
}
