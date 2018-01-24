package org.dancres.paxos.impl;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import org.dancres.paxos.Listener;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

public class ALNonRecoveryTest {
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

        _node1 = new ServerDispatcher(new HowlLogger(_node1Log), Listener.NULL_LISTENER);
        _node2 = new ServerDispatcher(new HowlLogger(_node2Log), Listener.NULL_LISTENER);
        _tport1 = new DropTransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _node1.init(_tport1);

        _tport2 = new DropTransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
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

        _node3 = new ServerDispatcher(new HowlLogger(_node3Log), Listener.NULL_LISTENER);
        _tport3 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _node3.init(_tport3);

        _node3.getAcceptorLearner().setRecoveryGracePeriod(5000);

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
        Thread.sleep(15000);

        // As recovery won't have been done the low watermarks should be different
        //
        Assert.assertTrue(_node2.getAcceptorLearner().getLowWatermark().getSeqNum() !=
        	_node3.getAcceptorLearner().getLowWatermark().getSeqNum());

        Assert.assertFalse(_node3.getCore().getCommon().getNodeState().test(NodeState.State.RECOVERING));

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

    static class DropTransportImpl extends TransportImpl {
        DropTransportImpl(MessageBasedFailureDetector anFD) throws Exception {
            super(anFD);
        }

        public void channelRead0(ChannelHandlerContext aContext, final DatagramPacket aPacket) {
            byte[] myBytes = new byte[aPacket.content().readableBytes()];

            aPacket.content().getBytes(0, myBytes);
            Packet myPacket = getPickler().unpickle(myBytes);

            if (myPacket.getMessage().getType() != PaxosMessage.Types.NEED) {
                super.channelRead0(aContext, aPacket);
            }

            // Ignore need requests to cause _node3 to timeout
        }
    }
}
