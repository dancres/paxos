package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryNetwork;

import java.net.InetSocketAddress;
import java.util.Random;

public interface Environment {
    Random getRng();

    NodeAdmin.Memento killAtRandom();

    boolean makeCurrent(NodeAdmin anAdmin);

    void addNodeAdmin(NodeAdmin.Memento aMemento);

    long getSettleCycles();

    OrderedMemoryNetwork getFactory();

    long getMaxCycles();

    boolean isLive();

    void settle();

    void terminate();

    long getDropCount();

    long getRxCount();

    long getTxCount();

    boolean validate();

    NodeAdmin getCurrentLeader();

    void updateLeader(InetSocketAddress anAddress);

    void doneOp();
}
