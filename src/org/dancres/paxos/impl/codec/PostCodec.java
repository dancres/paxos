package org.dancres.paxos.impl.codec;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.io.mina.Post;
import org.dancres.paxos.impl.core.messages.Operations;

public class PostCodec implements Codec {
    public IoBuffer encode(Object anObject) {
        Post myPost = (Post) anObject;
        byte[] myBytes = myPost.getData();

        IoBuffer myBuffer = IoBuffer.allocate(8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(4 + myBytes.length);
        myBuffer.putInt(Operations.POST);
        myBuffer.put(myBytes);
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(IoBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        int myArrLength = aBuffer.getInt() - 4;

        // Discard type
        aBuffer.getInt();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        return new Post(myBytes);
    }
}
