package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;

import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Random;

public interface Environment {
    Random getRng();

    Deque<NodeAdmin> getKillableNodes();

    NodeAdmin.Memento killSpecific(NodeAdmin anAdmin);

    boolean makeCurrent(NodeAdmin anAdmin);

    void addNodeAdmin(NodeAdmin.Memento aMemento);

    long getSettleCycles();

    OrderedMemoryNetwork getFactory();

    long getMaxCycles();

    boolean isReady();

    boolean isLive();

    void settle();

    void terminate();

    boolean validate();

    NodeAdmin getCurrentLeader();

    void updateLeader(InetSocketAddress anAddress);

    void doneOp();

    long getDoneOps();

    long getNextCkptOp();

    boolean isSettling();

    Permuter<OrderedMemoryTransportImpl.Context> getPermuter();
}
