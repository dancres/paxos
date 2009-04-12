package org.dancres.paxos.impl.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class NodeId {
    public static long from(InetSocketAddress anAddr) {
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

    public static InetSocketAddress toAddress(long aNodeId) throws Exception {
        byte[] myAddrBytes = new byte[4];
        int myPort = (int) aNodeId & 0xFFFFFFFF;

        long myAddr = (aNodeId >> 32) & 0xFFFFFFFF;

        for (int i = 3; i > -1; i--) {
            myAddrBytes[i] = (byte) (myAddr & 0xFF);
            myAddr = myAddr >> 8;
        }

        return new InetSocketAddress(InetAddress.getByAddress(myAddrBytes), myPort);
    }
}
