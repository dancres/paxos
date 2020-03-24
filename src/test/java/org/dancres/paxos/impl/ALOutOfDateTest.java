package org.dancres.paxos.impl;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import org.dancres.paxos.*;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.OutOfDate;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.test.utils.Builder;
import org.dancres.paxos.test.utils.FileSystem;
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

    private TransportImpl _tport1;
    private TransportImpl _tport2;
    private TransportImpl _tport3;

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(_node1Log));
        FileSystem.deleteDirectory(new File(_node2Log));
        FileSystem.deleteDirectory(new File(_node3Log));

        _tport1 = new OODTransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport2 = new OODTransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));

        Builder myBuilder = new Builder();

        myBuilder.newCoreWith(new HowlLogger(_node1Log), _tport1);
        myBuilder.newCoreWith(new HowlLogger(_node2Log), _tport2);
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

        Listener myListener = new Listener();

        Builder myBuilder = new Builder();

        _tport3 = myBuilder.newDefaultTransport();

        Core myCore = myBuilder.newCoreWith(new HowlLogger(_node3Log), _tport3);

        myCore.add(myListener);

        FDUtil.ensureFD(_tport3.getFD());

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
            myCore.submit(myProp, anOutcome -> {});
        } catch (InactiveException anIE) {
            isInactive = true;
        }

        Assert.assertTrue(isInactive);

        boolean stateChecked = false;

        try {
            myCore.checkpoint().forSaving();
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

        boolean didOOD() {
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
        OODTransportImpl(MessageBasedFailureDetector anFD) throws Exception {
            super(anFD);
        }

        public void channelRead0(ChannelHandlerContext aContext, final DatagramPacket aPacket) {
            byte[] myBytes = new byte[aPacket.content().readableBytes()];

            aPacket.content().getBytes(0, myBytes);
            Packet myPacket = getPickler().unpickle(myBytes);

            if (myPacket.getMessage().getType() != PaxosMessage.Types.NEED) {
                super.channelRead0(aContext, aPacket);
                return;
            }

            send(getPickler().newPacket(new OutOfDate()), myPacket.getSource());
        }
    }
}
