package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.OutOfDate;

import java.nio.ByteBuffer;

public class OutOfDateCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        ByteBuffer myBuffer;

        myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(Operations.OUTOFDATE);

        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        aBuffer.getInt();

        return new OutOfDate();
    }
}
