package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;

import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Iterator;
import java.util.Random;

public interface Environment {
    Random getRng();

    Deque<NodeAdmin> getNodes();

    NodeAdmin.Memento killSpecific(NodeAdmin anAdmin);

    boolean makeCurrent(NodeAdmin anAdmin);

    void addNodeAdmin(NodeAdmin.Memento aMemento);

    long getSettleCycles();

    OrderedMemoryNetwork getFactory();

    OrderedMemoryTransportImpl.RoutingDecisions getDecisionMaker();

    long getMaxCycles();

    boolean isLive();

    void settle();

    void terminate();

    long getDropCount();

    long getRxCount();

    long getTxCount();

    long getTempDeathCount();

    boolean validate();

    NodeAdmin getCurrentLeader();

    void updateLeader(InetSocketAddress anAddress);

    void doneOp();

    long getDoneOps();

    long getNextCkptOp();

    boolean isSettling();
}
