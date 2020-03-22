package org.dancres.paxos.test.utils;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Listener;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.test.net.Submitter;

public class Builder {
    public Builder() {
    }

    public Core newDefaultCore() {
        return new Core(new MemoryLogStorage(), CheckpointHandle.NO_CHECKPOINT, Listener.NULL_LISTENER, false);
    }

    public TransportImpl newDefaultTransport() throws Exception {
        return new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
    }

    public TransportImpl newDefaultStack() throws Exception {
        TransportImpl myTransport = newDefaultTransport();
        Core myCore = newDefaultCore().init(myTransport);
        myTransport.filterRx(new Submitter(myCore));

        return myTransport;
    }
}
