package org.dancres.paxos.messages.codec;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class Codecs {
    private static final Map<Integer, Codec> CODECS =
            Collections.unmodifiableMap(new HashMap<Integer, Codec>() {{
                put(Operations.HEARTBEAT, new HeartbeatCodec());
                put(Operations.OUTOFDATE, new OutOfDateCodec());
                put(Operations.ENVELOPE, new EnvelopeCodec());
                put(Operations.COLLECT, new CollectCodec());
                put(Operations.LAST, new LastCodec());
                put(Operations.BEGIN, new BeginCodec());
                put(Operations.ACCEPT, new AcceptCodec());
                put(Operations.SUCCESS, new SuccessCodec());
                put(Operations.OLDROUND, new OldRoundCodec());
                put(Operations.NEED, new NeedCodec());
                put(Operations.EVENT, new VoteOutcomeCodec());
            }});

    public static byte[] encode(PaxosMessage aMessage) {
        return CODECS.get(aMessage.getType()).encode(aMessage).array();
    }

    public static PaxosMessage decode(byte[] aBuffer) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aBuffer);

        return (PaxosMessage) CODECS.get(myBuffer.getInt(0)).decode(myBuffer);
    }

    public static long flatten(InetSocketAddress anAddr) {
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

    public static InetSocketAddress expand(long anAddr) {
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
