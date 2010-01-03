package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.PaxosMessage;

public class Codecs {
    public static final Codec[] CODECS = new Codec[] {
        new HeartbeatCodec(), new EmptyCodec(), new PostCodec(), new CollectCodec(), new LastCodec(),
            new BeginCodec(), new AcceptCodec(), new SuccessCodec(), new AckCodec(), new OldRoundCodec(),
            new ProposerReqCodec(), new FailCodec(), new CompleteCodec()
    };

    public static byte[] encode(PaxosMessage aMessage) {
        int myType = aMessage.getType();
        Codec myCodec = Codecs.CODECS[myType];

        return myCodec.encode(aMessage).array();
    }

    public static PaxosMessage decode(byte[] aBuffer, boolean hasLength) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aBuffer);
        int myOp;
        
        if (hasLength)
            myOp = myBuffer.getInt(4);
        else
            myOp = myBuffer.getInt(0);

        Codec myCodec = Codecs.CODECS[myOp];

        return (PaxosMessage) myCodec.decode(myBuffer);
    }

}
