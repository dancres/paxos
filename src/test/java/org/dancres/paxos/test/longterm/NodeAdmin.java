package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.test.net.OrderedMemoryNetwork.OrderedMemoryTransport;

import java.net.InetSocketAddress;

public interface NodeAdmin {

    void settle();

    interface Memento {
        Object getContext();
        InetSocketAddress getAddress();
    }

    OrderedMemoryTransport getTransport();
    void checkpoint() throws Exception;
    CheckpointStorage.ReadCheckpoint getLastCheckpoint();
    long lastCheckpointTime();
    boolean isOutOfDate();
    Memento terminate();
    boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt);
    long getLastSeq();
}
