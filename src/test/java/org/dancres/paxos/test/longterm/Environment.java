package org.dancres.paxos.test.longterm;

import java.util.Random;

public interface Environment {
    Random getRng();

    NodeAdmin.Memento killAtRandom();

    boolean makeCurrent(NodeAdmin anAdmin);

    public void addNodeAdmin(NodeAdmin.Memento aMemento);
}
