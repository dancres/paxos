package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.impl.Transport;

import java.net.InetSocketAddress;
import java.util.Random;

public interface NodeAdmin {

    void settle();

    interface Memento {
        Object getContext();
        InetSocketAddress getAddress();
    }

    Transport getTransport();
    void checkpoint() throws Exception;
    CheckpointStorage.ReadCheckpoint getLastCheckpoint();
    long lastCheckpointTime();
    boolean isOutOfDate();
    Memento terminate();
    boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt);
    long getLastSeq();
    Random getRngByName(String aName);
}
