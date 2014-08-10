package org.dancres.paxos.test.longterm;

import java.util.Random;

public interface Environment {
    Random getRng();

    void killAtRandom();

    boolean makeCurrent(NodeAdmin anAdmin);
}
