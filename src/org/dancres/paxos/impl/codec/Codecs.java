package org.dancres.paxos.impl.codec;

public class Codecs {
    public static final Codec[] CODECS = new Codec[] {
        new HeartbeatCodec(), new EmptyCodec(), new PostCodec(), new CollectCodec(), new LastCodec(),
            new BeginCodec(), new AcceptCodec(), new SuccessCodec(), new AckCodec(), new OldRoundCodec(),
            new ProposerReqCodec(), new FailCodec()
    };
}
