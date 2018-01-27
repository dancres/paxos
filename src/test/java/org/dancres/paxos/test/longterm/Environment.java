package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryNetwork;

import java.util.Random;

public interface Environment {
    Random getRng();

    void addNodeAdmin(NodeAdmin.Memento aMemento);

    long getSettleCycles();

    OrderedMemoryNetwork getFactory();

    long getMaxCycles();

    boolean isReady();

    boolean isLive();

    void settle();

    void shutdown();

    NodeSet getNodes();

    void doneOp();

    long getDoneOps();

    boolean isSettling();
}
