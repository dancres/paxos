package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;

public class ClusterMemberIntroTest {
    private static final String _node1Log = "node1logs";
    private static final String _node2Log = "node2logs";
    private static final String _node3Log = "node3logs";

    private ServerDispatcher _node1;
    private ServerDispatcher _node2;
    private ServerDispatcher _node3;

    private TransportImpl _tport1;
    private TransportImpl _tport2;
    private TransportImpl _tport3;

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(_node1Log));
        FileSystem.deleteDirectory(new File(_node2Log));
        FileSystem.deleteDirectory(new File(_node3Log));

        Runtime.getRuntime().runFinalizersOnExit(true);

        _node1 = new ServerDispatcher(new HowlLogger(_node1Log));
        _node2 = new ServerDispatcher(new HowlLogger(_node2Log));
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _node2.init(_tport2);
    }

    @After
    public void stop() throws Exception {
        _tport1.terminate();
        _tport2.terminate();

        if (_tport3 != null)
            _tport3.terminate();
    }

    @Test
    public void intro() throws Exception {
        ClientDispatcher myClient = new ClientDispatcher();
        TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        // Lock down membership
        //
        _tport1.getFD().pin(_tport1.getFD().getMembers().getMembers().keySet());
        _tport2.getFD().pin(_tport2.getFD().getMembers().getMembers().keySet());

        // Run some instances
        //
        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);
            Proposal myProp = new Proposal("data", myBuffer.array());

            myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

            VoteOutcome myEv = myClient.getNext(10000);

            Assert.assertFalse((myEv == null));

            Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);

            Assert.assertTrue(myEv.getSeqNum() == i);
        }

        // Let things settle, then "bring up" additional node that is out of date and not a member
        //
        Thread.sleep(2000);

        _node3 = new ServerDispatcher(new HowlLogger(_node3Log));
        _tport3 = new TransportImpl(new FailureDetectorImpl(5000));
        _node3.init(_tport3);
        _node3.getAcceptorLearner().setRecoveryGracePeriod(1000);

        // New instance should achieve no membership
        //
        Assert.assertFalse(FDUtil.testFD(_tport3.getFD(), 10000));
        Assert.assertFalse(_tport3.getFD().isMember(_tport3.getLocalAddress()));

        // Run another instance should cause _node3 to run a recovery as it completes
        //
        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(67);
        Proposal myProp = new Proposal("data", myBuffer.array());

        myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

        VoteOutcome myMsg = myClient.getNext(10000);

        // Wait for recovery to run
        //
        Thread.sleep(5000);

        // Node 3 should now have same low watermark as the other nodes & performed one recovery cycle
        //
        Assert.assertTrue(_node2.getAcceptorLearner().getLowWatermark().getSeqNum() ==
                _node3.getAcceptorLearner().getLowWatermark().getSeqNum());
        Assert.assertEquals(1, _node3.getAcceptorLearner().getStats().getRecoveryCycles());

        // Node 3 should still have no membership
        //
        Assert.assertFalse(_tport3.getFD().isMember(_tport3.getLocalAddress()));

        // Leader should not see node 3 as a member
        //
        Assert.assertFalse(_tport2.getFD().isMember(_tport3.getLocalAddress()));

        // Run another cycle
        //
        myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

        myMsg = myClient.getNext(10000);

        Thread.sleep(2000);

        // Node 3 should have same watermark, no active accepts, one recovery cycle
        //
        Assert.assertTrue(_node2.getAcceptorLearner().getLowWatermark().getSeqNum() ==
                _node3.getAcceptorLearner().getLowWatermark().getSeqNum());
        Assert.assertEquals(1, _node3.getAcceptorLearner().getStats().getRecoveryCycles());
        Assert.assertEquals(0, _node3.getAcceptorLearner().getStats().getActiveAccepts());

        // Promote node 3
        //
        Collection<InetSocketAddress> myNewMembers = new LinkedList<>();
        myNewMembers.addAll(_tport2.getFD().getMembers().getMembers().keySet());
        myNewMembers.add(_tport3.getLocalAddress());

        Assert.assertTrue(_node2.getCore().updateMembership(myNewMembers));

        Thread.sleep(2000);

        // Node 3 should have membership
        //
        Assert.assertTrue(_tport3.getFD().isMember(_tport3.getLocalAddress()));

        // Run another cycle
        //
        myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

        myMsg = myClient.getNext(10000);

        Thread.sleep(2000);

        // Node 3 should have same watermark, one active accept, one recovery cycle
        //
        Assert.assertTrue(_node2.getAcceptorLearner().getLowWatermark().getSeqNum() ==
                _node3.getAcceptorLearner().getLowWatermark().getSeqNum());
        Assert.assertEquals(1, _node3.getAcceptorLearner().getStats().getRecoveryCycles());
        Assert.assertEquals(1, _node3.getAcceptorLearner().getStats().getActiveAccepts());

        myTransport.terminate();
    }
}
