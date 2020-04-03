package org.dancres.paxos.test.utils;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Listener;
import org.dancres.paxos.LogStorage;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
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

    public TransportImpl newRxFilteredTransport(Transport.Filter aFilter) throws Exception {
        TransportImpl myTransport = newDefaultTransport();

        myTransport.filterRx(aFilter);
        return myTransport;
    }

    public TransportImpl newTransportWith(MessageBasedFailureDetector anFD) throws Exception {
        return new TransportImpl(anFD);
    }

    public Core newCoreWith(LogStorage aLogger, Transport aTransport) throws Exception {
        Core myCore =  new Core(aLogger, CheckpointHandle.NO_CHECKPOINT, Listener.NULL_LISTENER, false);
        myCore.init(aTransport);
        aTransport.filterRx(new Submitter(myCore));

        return myCore;
    }

    public Core newCoreWith(LogStorage aLogger, CheckpointHandle aHandle,
                            Listener aListener, Transport aTransport) throws Exception {
        Core myCore = new Core(aLogger, aHandle, aListener, false);
        myCore.init(aTransport);
        aTransport.filterRx(new Submitter(myCore));

        return myCore;
    }

    public Core newNonHeartbeatingCoreWith(LogStorage aLogger, Transport aTransport) throws Exception {
        Core myCore =  new Core(aLogger, CheckpointHandle.NO_CHECKPOINT, Listener.NULL_LISTENER, true);
        myCore.init(aTransport);
        aTransport.filterRx(new Submitter(myCore));

        return myCore;
    }

    public Core newCoreWith(Transport aTransport) throws Exception {
        Core myCore = newDefaultCore().init(aTransport);
        aTransport.filterRx(new Submitter(myCore));

        return myCore;
    }

    public TransportImpl newDefaultStack() throws Exception {
        TransportImpl myTransport = newDefaultTransport();
        Core myCore = newDefaultCore().init(myTransport);
        myTransport.filterRx(new Submitter(myCore));

        return myTransport;
    }
}
