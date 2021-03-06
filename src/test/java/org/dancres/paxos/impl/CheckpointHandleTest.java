package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.PicklerImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Learned;
import org.dancres.paxos.test.net.*;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointHandleTest {
    private static final String DIRECTORY = "howllogs";
    private static final byte[] HANDBACK = new byte[] {1, 2, 3, 4};

    private final InetSocketAddress _nodeId = TestAddresses.next();
    private final InetSocketAddress _broadcastId = TestAddresses.next();

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(DIRECTORY));
    }

    @Test
    public void test() throws Exception {
        PicklerImpl myPickler = new PicklerImpl();

        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        ALTestTransportImpl myTransport = new ALTestTransportImpl(_nodeId, _broadcastId,
                new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        Common myCommon = new Common().setTransport(myTransport);

        AcceptorLearner myAl = new AcceptorLearner(myLogger, myCommon);
        myAl.open(CheckpointHandle.NO_CHECKPOINT);

        Assert.assertFalse(myCommon.getNodeState().test(NodeState.State.RECOVERING));

        long myRndNum = 1;
        long mySeqNum = 0;

        CheckpointHandle myFirstHandle = myAl.newCheckpoint();
        
        // First collect, Al has no state so this is accepted and will be held in packet buffer
        //
        myAl.processMessage(myPickler.newPacket(new Collect(mySeqNum, myRndNum), _nodeId));

        PaxosMessage myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == PaxosMessage.Types.LAST);

        // Now push a value into the Al, also held in packet buffer
        //
        byte[] myData = new byte[] {1};
        Proposal myValue = new Proposal();
        myValue.put("data", myData);
        myValue.put("handback", HANDBACK);

        myAl.processMessage(myPickler.newPacket(new Begin(mySeqNum, myRndNum, myValue), _nodeId));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == PaxosMessage.Types.ACCEPT);

        // Commit this instance
        //
        myAl.processMessage(myPickler.newPacket(new Learned(mySeqNum, myRndNum), _nodeId));

        CheckpointHandle mySecondHandle = myAl.newCheckpoint();
        
        Assert.assertTrue((mySecondHandle.isNewerThan(myFirstHandle)));

        // Execute and commit another instance
        //
        myAl.processMessage(myPickler.newPacket(new Begin(mySeqNum + 1, myRndNum, myValue), _nodeId));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == PaxosMessage.Types.ACCEPT);

        // Commit this instance
        //
        myAl.processMessage(myPickler.newPacket(new Learned(mySeqNum + 1, myRndNum), _nodeId));

        CheckpointHandle myThirdHandle = myAl.newCheckpoint();

        Assert.assertTrue((myThirdHandle.isNewerThan(mySecondHandle)));
        
        Assert.assertFalse(myFirstHandle.isNewerThan(myThirdHandle));
        
        Assert.assertFalse(mySecondHandle.isNewerThan(myThirdHandle));

        myAl.close();
    }
}

