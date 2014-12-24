package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Map;

public interface Membership {
    public interface MetaData {
        public byte[] getData();
        public long getTimestamp();
    }

    public Map<InetSocketAddress, MetaData> getMembers();
    public byte[] dataForNode(InetSocketAddress anAddress);
}
