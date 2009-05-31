package org.dancres.paxos.impl.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class NodeId {
    public static final NodeId BROADCAST = new NodeId(Long.MAX_VALUE);

    private Long _flattenedAddress;

    private NodeId(long aFlattenedAddress) {
        _flattenedAddress = new Long(aFlattenedAddress);
    }

    public static NodeId from(SocketAddress anAddr) {
        InetSocketAddress myAddr = (InetSocketAddress) anAddr;

        byte[] myAddress = myAddr.getAddress().getAddress();
        long myNodeId = 0;

        // Only cope with IPv4 right now
        //
        assert (myAddress.length == 4);

        for (int i = 0; i < 4; i++) {
            myNodeId = myNodeId << 8;
            myNodeId |= (int) myAddress[i] & 0xFF;
        }

        myNodeId = myNodeId << 32;
        myNodeId |= myAddr.getPort();

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
        return _flattenedAddress.longValue();
    }

    public long asLong() {
        return getFlattenedAddress();
    }

    public String toString() {
        return Long.toHexString(_flattenedAddress.longValue());
    }

    public boolean leads(NodeId aNodeId) {
        return getFlattenedAddress() > aNodeId.getFlattenedAddress();
    }

    public int hashCode() {
        return _flattenedAddress.hashCode();
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof NodeId) {
            NodeId myOther = (NodeId) anObject;

            return (myOther.getFlattenedAddress() == getFlattenedAddress());
        }

        return false;
    }
}
