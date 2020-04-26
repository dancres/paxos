package org.dancres.paxos.impl;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Paxos;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.net.TestAddresses;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;

public class ALCheckpointTest {
    private static final String DIRECTORY = "howllogs";

    private final InetSocketAddress _nodeId = TestAddresses.next();
    private final InetSocketAddress _broadcastId = TestAddresses.next();

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(DIRECTORY));
    }

    @Test
    public void savingTest() throws Exception {
        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        ALTestTransportImpl myTransport = new ALTestTransportImpl(_nodeId, _broadcastId,
                new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));

        AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common().setTransport(myTransport));

        myAl.open(CheckpointHandle.NO_CHECKPOINT);
        Paxos.Checkpoint myCheckpoint = myAl.forSaving();
        myCheckpoint.getConsumer().apply(myCheckpoint.getHandle());
        myAl.close();

        myAl = new AcceptorLearner(myLogger, new Common().setTransport(myTransport));

        myAl.open(myCheckpoint.getHandle());
        myAl.close();

        ByteArrayOutputStream myBAOS = new ByteArrayOutputStream();
        ObjectOutputStream myOOS = new ObjectOutputStream(myBAOS);

        myOOS.writeObject(myCheckpoint.getHandle());
        myOOS.close();

        ByteArrayInputStream myBAIS = new ByteArrayInputStream(myBAOS.toByteArray());
        ObjectInputStream myOIS = new ObjectInputStream(myBAIS);
        CheckpointHandle myRecoveredHandle = (CheckpointHandle) myOIS.readObject();

        myAl = new AcceptorLearner(myLogger, new Common().setTransport(myTransport));

        myAl.open(myRecoveredHandle);
        myAl.close();
    }

    @Test
    public void recoveringTest() throws Exception {
        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        ALTestTransportImpl myTransport = new ALTestTransportImpl(_nodeId, _broadcastId,
                new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));

        AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common().setTransport(myTransport));

        myAl.open(CheckpointHandle.NO_CHECKPOINT);

        Paxos.Checkpoint myCheckpoint = myAl.forRecovery();

        tryIt(myCheckpoint, null);
        tryIt(myCheckpoint, myCheckpoint.getHandle());
        tryIt(myCheckpoint, CheckpointHandle.NO_CHECKPOINT);

        myAl.close();
    }

    private void tryIt(Paxos.Checkpoint aCheckpoint, CheckpointHandle aHandle) {
        boolean didException = false;

        try {
            aCheckpoint.getConsumer().apply(aHandle);
        } catch(Exception anE) {
            didException = true;
        }

        Assert.assertTrue(didException);
    }
}
