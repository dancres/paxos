package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Map;

public interface Membership {
    interface MetaData {
        byte[] getData();
    }

    Map<InetSocketAddress, MetaData> getMembers();
    byte[] dataForNode(InetSocketAddress anAddress);
}
