package org.dancres.paxos.impl.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class NodeId {
    private long _flattenedAddress;

    private NodeId(long aFlattenedAddress) {
        _flattenedAddress = aFlattenedAddress;
    }

    public static NodeId from(InetSocketAddress anAddr) {
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

        return new NodeId(myNodeId);
    }

    public static NodeId from(long aFlattenedNodeId) {
        return new NodeId(aFlattenedNodeId);
    }

    public static InetSocketAddress toAddress(NodeId aNodeId) throws Exception {
        byte[] myAddrBytes = new byte[4];
        int myPort = (int) aNodeId.getFlattenedAddress() & 0xFFFFFFFF;

        long myAddr = (aNodeId.getFlattenedAddress() >> 32) & 0xFFFFFFFF;

        for (int i = 3; i > -1; i--) {
            myAddrBytes[i] = (byte) (myAddr & 0xFF);
            myAddr = myAddr >> 8;
        }

        return new InetSocketAddress(InetAddress.getByAddress(myAddrBytes), myPort);
    }

    private long getFlattenedAddress() {
        return _flattenedAddress;
    }

    public long asLong() {
        return getFlattenedAddress();
    }

    public String toString() {
        return Long.toHexString(_flattenedAddress);
    }

    public boolean leads(NodeId aNodeId) {
        return _flattenedAddress > aNodeId.getFlattenedAddress();
    }
}
