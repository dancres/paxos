package org.dancres.paxos.test.longterm;

import org.dancres.paxos.test.net.OrderedMemoryNetwork;

import java.util.Random;

public interface Environment {
    Random getRng();

    void addNodeAdmin(NodeAdmin.Memento aMemento);

    OrderedMemoryNetwork getFactory();

    boolean isReady();

    boolean isSimulating();

    NodeSet getNodes();

    boolean isSettling();
}
