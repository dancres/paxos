package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;

public interface Membership {
    interface MetaData {
        byte[] getData();
        long getTimestamp();
    }

    Map<InetSocketAddress, MetaData> getMembers();
    byte[] dataForNode(InetSocketAddress anAddress);
    boolean updateMembership(Collection<InetSocketAddress> aMembers) throws InactiveException;
}
