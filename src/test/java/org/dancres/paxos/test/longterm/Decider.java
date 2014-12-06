package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;

interface Decider extends OrderedMemoryTransportImpl.RoutingDecisions {
    long getDropCount();

    long getRxPacketCount();

    long getTxPacketCount();

    long getTempDeathCount();

    void settle();
};
