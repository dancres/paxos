package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.Operations;

public class HeartbeatCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Heartbeat myHB = (Heartbeat) anObject;
        int myMetaDataLength = myHB.getMetaData().length;
        ByteBuffer myBuffer = ByteBuffer.allocate(16 + myMetaDataLength);

        myBuffer.putInt(Operations.HEARTBEAT);
        myBuffer.putInt(myMetaDataLength);
        myBuffer.putLong(Codecs.flatten(myHB.getNodeId()));
        myBuffer.put(myHB.getMetaData());
        myBuffer.flip();
        
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        int myMetaSize = aBuffer.getInt();
        long myAddr = aBuffer.getLong();

        byte[] myBytes = new byte[myMetaSize];
        aBuffer.get(myBytes);

        return new Heartbeat(Codecs.expand(myAddr), myBytes);
    }
}
