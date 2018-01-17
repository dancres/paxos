package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;

public class Codecs {
    private static final Map<Integer, Codec> CODECS =
            Map.ofEntries(Map.entry(PaxosMessage.Types.HEARTBEAT, new HeartbeatCodec()),
                    Map.entry(PaxosMessage.Types.OUTOFDATE, new OutOfDateCodec()),
                    Map.entry(PaxosMessage.Types.ENVELOPE, new EnvelopeCodec()),
                    Map.entry(PaxosMessage.Types.COLLECT, new CollectCodec()),
                    Map.entry(PaxosMessage.Types.LAST, new LastCodec()),
                    Map.entry(PaxosMessage.Types.BEGIN, new BeginCodec()),
                    Map.entry(PaxosMessage.Types.ACCEPT, new AcceptCodec()),
                    Map.entry(PaxosMessage.Types.LEARNED, new SuccessCodec()),
                    Map.entry(PaxosMessage.Types.OLDROUND, new OldRoundCodec()),
                    Map.entry(PaxosMessage.Types.NEED, new NeedCodec()),
                    Map.entry(PaxosMessage.Types.EVENT, new EventCodec()));

    public static byte[] encode(PaxosMessage aMessage) {
        return CODECS.get(aMessage.getType()).encode(aMessage).array();
    }

    public static PaxosMessage decode(byte[] aBuffer) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aBuffer);

        return (PaxosMessage) CODECS.get(myBuffer.getInt(0)).decode(myBuffer);
    }

    public static byte[] flatten(Collection<InetSocketAddress> aList) {
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + (8 * aList.size()));

        myBuffer.putInt(aList.size());

        for (InetSocketAddress myAddr : aList)
            myBuffer.putLong(flatten(myAddr));

        return myBuffer.array();
    }

    public static Collection<InetSocketAddress> expand(byte[] aListOfAddresses) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aListOfAddresses);
        int myNumAddr = myBuffer.getInt();
        LinkedList<InetSocketAddress> myAddrs = new LinkedList<>();

        for (int i = 0; i < myNumAddr; i++)
            myAddrs.add(expand(myBuffer.getLong()));

        return myAddrs;
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
        int myPort = (int) anAddr;

        long myAddr = (anAddr >> 32);

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
