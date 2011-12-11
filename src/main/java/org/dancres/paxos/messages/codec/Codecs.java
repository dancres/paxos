package org.dancres.paxos.messages.codec;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.dancres.paxos.messages.PaxosMessage;

public class Codecs {
    public static final Codec[] CODECS = new Codec[] {
        new HeartbeatCodec(), new OutOfDateCodec(), new EnvelopeCodec(), new CollectCodec(), new LastCodec(),
            new BeginCodec(), new AcceptCodec(), new SuccessCodec(), new EmptyCodec(), new OldRoundCodec(),
            new NeedCodec(), new VoteOutcomeCodec()
    };

    public static byte[] encode(PaxosMessage aMessage) {
        int myType = aMessage.getType();
        Codec myCodec = Codecs.CODECS[myType];

        return myCodec.encode(aMessage).array();
    }

    public static PaxosMessage decode(byte[] aBuffer) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aBuffer);
        int myOp;
        
		myOp = myBuffer.getInt(0);

        Codec myCodec = Codecs.CODECS[myOp];

        return (PaxosMessage) myCodec.decode(myBuffer);
    }

    static long flatten(InetSocketAddress anAddr) {
        byte[] myAddress = anAddr.getAddress().getAddress();
        long myNodeId = 0;

        // Only cope with IPv4 right now
        //
        assert (myAddress.length == 4);

        for (int i = 0; i < 4; i++) {
            myNodeId = myNodeId << 8;
            myNodeId |= (int) myAddress[i] & 0xFF;
        }

        myNodeId = myNodeId << 32;
        myNodeId |= anAddr.getPort();

        return myNodeId;        
    }

    static InetSocketAddress expand(long anAddr) {
        byte[] myAddrBytes = new byte[4];
        int myPort = (int) anAddr & 0xFFFFFFFF;

        long myAddr = (anAddr >> 32) & 0xFFFFFFFF;

        for (int i = 3; i > -1; i--) {
            myAddrBytes[i] = (byte) (myAddr & 0xFF);
            myAddr = myAddr >> 8;
        }

        try {
            return new InetSocketAddress(InetAddress.getByAddress(myAddrBytes), myPort);
        } catch (UnknownHostException aUHE) {
            throw new IllegalArgumentException("Can't convert to an address: " + anAddr, aUHE);
        }
    }
}
