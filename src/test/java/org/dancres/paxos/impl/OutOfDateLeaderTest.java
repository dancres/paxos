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
import java.nio.ByteBuffer;

public class OutOfDateLeaderTest {
    private static final String _node2Log = "node2logs";
    private static final String _node1Log = "node1logs";
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

        Leader.LeaseDuration.set(10000);

        _node1 = new ServerDispatcher(new HowlLogger(_node1Log), true);
        _node2 = new ServerDispatcher(new HowlLogger(_node2Log), true);
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport2.routeTo(_node2);
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
    public void post() throws Exception {
        ClientDispatcher myClient = new ClientDispatcher();
        TransportImpl myTransport = new TransportImpl(null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        System.err.println("Run some instances");

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

        // Let things settle, then "bring up" additional node that is out of date
        //
        Thread.sleep(5000);

        System.err.println("Start node3");

        _node3 = new ServerDispatcher(new HowlLogger(_node3Log), true);
        _tport3 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport3.routeTo(_node3);
        _node3.init(_tport3);
        _node3.getAcceptorLearner().setRecoveryGracePeriod(1000);

        FDUtil.ensureFD(_tport3.getFD());

        System.err.println("Run another instance - trigger");

        // Need current leader lease to expire
        //
        Thread.sleep(Leader.LeaseDuration.get() + 1000);

        // Run another instance should cause _node3 to fail with OTHER_LEADER
        //
        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(67);
        Proposal myProp = new Proposal("data", myBuffer.array());

        myClient.send(new Envelope(myProp), _tport3.getLocalAddress());

        VoteOutcome myMsg = myClient.getNext(10000);

        Assert.assertEquals(VoteOutcome.Reason.OTHER_LEADER, myMsg.getResult());

        myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(69);
        Proposal myProp2 = new Proposal("data", myBuffer.array());

        System.err.println("Run another instance - in progress");

        // New leader should be up to date as result of receiving OLD_ROUND - this will also cause AL to recover
        //
        myClient.send(new Envelope(myProp2), _tport3.getLocalAddress());

        myMsg = myClient.getNext(10000);

        /*
         * If recovery progresses quickly enough, leader will get decision otherwise it'll be timeout.
         * This is because the recovering node will indicate presence in FD membership and leader will wait for
         * response. When it doesn't get it, there will be a timeout announced by watchdog causing a fail and in turn
         * user code would normally retry.
         */
        Assert.assertTrue((VoteOutcome.Reason.VALUE == myMsg.getResult()) ||
                (VoteOutcome.Reason.VOTE_TIMEOUT == myMsg.getResult()));

        System.err.println("Wait for settle");

        // Wait for recovery to run
        //
        Thread.sleep(10000);

        // _node3 should now have same low watermark as the other nodes
        //
        Assert.assertEquals("Watermarks aren't equal, recovery fail?",
                _node2.getAcceptorLearner().getLowWatermark().getSeqNum(),
                _node3.getAcceptorLearner().getLowWatermark().getSeqNum());


        /*
         *  Let things settle before we close them off otherwise we can get a false assertion in the AL. This is
         *  because there are two nodes at play. Leader achieves success and posts this to both AL's, the first
         *  receives it and announces it to the test code which then completes and shuts things down before the
         *  second AL has finished processing the same success, by which time the log has been shut in that AL causing
         *  the assertion.
         */
        System.err.println("Shutdown");

        myTransport.terminate();
    }

    public static void main(String[] anArgs) throws Exception {
        ALRecoveryTest myTest = new ALRecoveryTest();

        myTest.init();
        myTest.post();
        myTest.stop();
    }
}
