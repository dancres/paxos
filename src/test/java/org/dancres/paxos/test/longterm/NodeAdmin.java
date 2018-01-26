package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;

import java.net.InetSocketAddress;
import java.util.Random;

public interface NodeAdmin {

    void settle();

    interface Memento {
        Object getContext();
        InetSocketAddress getAddress();
    }

    OrderedMemoryTransportImpl getTransport();
    void checkpoint() throws Exception;
    CheckpointStorage.ReadCheckpoint getLastCheckpoint();
    long lastCheckpointTime();
    boolean isOutOfDate();
    Memento terminate();
    boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt);
    long getLastSeq();
    Random getRngByName(String aName);
}
