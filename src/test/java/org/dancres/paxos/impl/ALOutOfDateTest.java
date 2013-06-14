package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.OutOfDate;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.test.utils.FileSystem;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

public class ALOutOfDateTest {
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

        _node1 = new ServerDispatcher(new FailureDetectorImpl(5000), new HowlLogger(_node1Log));
        _node2 = new ServerDispatcher(new FailureDetectorImpl(5000), new HowlLogger(_node2Log));
        _tport1 = new OODTransportImpl();
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new OODTransportImpl();
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
        TransportImpl myTransport = new TransportImpl();
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        FDUtil.ensureFD(_node1.getCore().getCommon().getPrivateFD());
        FDUtil.ensureFD(_node2.getCore().getCommon().getPrivateFD());

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

        Listener myListener = new Listener();

        _node3 = new ServerDispatcher(new FailureDetectorImpl(5000), new HowlLogger(_node3Log));
        _tport3 = new TransportImpl();
        _tport3.routeTo(_node3);
        _node3.init(_tport3);
        _node3.add(myListener);

        FDUtil.ensureFD(_node3.getCore().getCommon().getPrivateFD());

        System.err.println("Run another instance - trigger");

        // Run another instance should cause _node3 to run a recovery as it completes
        //
        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(67);

        Proposal myProp = new Proposal("data", myBuffer.array());
        
        myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

        VoteOutcome myOutcome = myClient.getNext(10000);

        System.err.println("Wait for settle");

        // Wait for recovery to run to completion
        //
        Thread.sleep(5000);

        // _node3 should have signalled OutOfDate
        //
        Assert.assertTrue(myListener.didOOD());
        
        // node3 should refuse submissions
        //
        boolean isInactive = false;
        
        try {
            _node3.getCore().submit(myProp, new Completion<VoteOutcome>() {
                public void complete(VoteOutcome anOutcome) {
                }
            });
        } catch (InactiveException anIE) {
            isInactive = true;
        }

        Assert.assertTrue(isInactive);

        boolean stateChecked = false;

        try {
            _node3.getCore().newCheckpoint();
        } catch (IllegalStateException anISE) {
            stateChecked = true;
        }

        Assert.assertTrue(stateChecked);

        /*
         *  Let things settle before we close them off otherwise we can get a false assertion in the AL. This is
         *  because there are two nodes at play. Leader achieves success and posts this to both AL's, the first
         *  receives it and announces it to the test code which then completes and shuts things down before the
         *  second AL has finished processing the same success, by which time the log has been shut in that AL causing
         *  the assertion.
         */
        Thread.sleep(5000);

        System.err.println("Shutdown");

        myTransport.terminate();
    }

    public static void main(String[] anArgs) throws Exception {
        ALOutOfDateTest myTest = new ALOutOfDateTest();

        myTest.init();
        myTest.post();
        myTest.stop();
    }

    static class Listener implements org.dancres.paxos.Listener {
        private volatile boolean _receivedOOD = false;

        public boolean didOOD() {
            return _receivedOOD;
        }

        public void transition(StateEvent anEvent) {
            if (anEvent.getResult() == StateEvent.Reason.OUT_OF_DATE) {
                System.err.println("OOD Received from AL");
                _receivedOOD = true;
            }
        }
    }

    static class OODTransportImpl extends TransportImpl {
        OODTransportImpl() throws Exception {
            super();
        }

        public void messageReceived(ChannelHandlerContext aContext, MessageEvent anEvent) {
            Packet myPacket = (Packet) anEvent.getMessage();

            if (myPacket.getMessage().getType() != Operations.NEED) {
                super.messageReceived(aContext, anEvent);
                return;
            }

            send(new OutOfDate(), myPacket.getSource());
        }
    }
}
