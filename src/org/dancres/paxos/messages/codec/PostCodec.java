package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.Post;

public class PostCodec implements Codec {
    public ByteBuffer encode(Object anObject) {
        Post myPost = (Post) anObject;
        byte[] myBytes = myPost.getConsolidatedValue().marshall();

        ByteBuffer myBuffer = ByteBuffer.allocate(8 + myBytes.length);

        // Length count does not include length bytes themselves
        //
        myBuffer.putInt(4 + myBytes.length);
        myBuffer.putInt(Operations.POST);
        myBuffer.put(myBytes);
        myBuffer.flip();
        return myBuffer;
    }

    public Object decode(ByteBuffer aBuffer) {
        // Discard the length and operation so remaining data can be processed
        // separately
        int myArrLength = aBuffer.getInt() - 4;

        // Discard type
        aBuffer.getInt();

        byte[] myBytes = new byte[myArrLength];
        aBuffer.get(myBytes);
        return new Post(new ConsolidatedValue(myBytes));
    }
}
