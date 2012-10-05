package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.Stream;
import org.dancres.paxos.impl.AcceptorLearner;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.dancres.paxos.test.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointHandleTest {
    private static final String DIRECTORY = "howllogs";
    private static byte[] HANDBACK = new byte[] {1, 2, 3, 4};

    private InetSocketAddress _nodeId = Utils.getTestAddress();
    private InetSocketAddress _broadcastId = Utils.getTestAddress();

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(DIRECTORY));
    }

    private static class TransportImpl implements Transport {
        private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
        private InetSocketAddress _nodeId;
        private InetSocketAddress _broadcastId;

        TransportImpl(InetSocketAddress aNodeId, InetSocketAddress aBroadcastId) {
            _nodeId = aNodeId;
            _broadcastId = aBroadcastId;
        }

        public Transport.PacketPickler getPickler() {
            return null;
        }

        public void add(Dispatcher aDispatcher) {
        }

        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
            synchronized(_messages) {
                _messages.add(aMessage);
                _messages.notifyAll();
            }
        }

        PaxosMessage getNextMsg() {
            synchronized(_messages) {
                while (_messages.size() == 0) {
                    try {
                        _messages.wait();
                    } catch (InterruptedException anIE) {
                        // Ignore
                    }
                }

                return _messages.remove(0);
            }
        }

        public InetSocketAddress getLocalAddress() {
            return _nodeId;
        }

        public InetSocketAddress getBroadcastAddress() {
            return _broadcastId;
        }

        public void shutdown() {
        }

        public void connectTo(InetSocketAddress aNodeId, ConnectionHandler aHandler) {
        }
    }

    @Test
    public void test() throws Exception {
        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        TransportImpl myTransport = new TransportImpl(_nodeId, _broadcastId);
        Common myCommon = new Common(myTransport, new NullFailureDetector());

        AcceptorLearner myAl = new AcceptorLearner(myLogger, myCommon);
        myAl.open(CheckpointHandle.NO_CHECKPOINT);

        Assert.assertFalse(myCommon.testState(Common.FSMStates.RECOVERING));

        long myRndNum = 1;
        long mySeqNum = 0;

        CheckpointHandle myFirstHandle = myAl.newCheckpoint();
        
        // First collect, Al has no state so this is accepted and will be held in packet buffer
        //
        myAl.messageReceived(new Collect(mySeqNum, myRndNum, _nodeId));

        PaxosMessage myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.LAST);

        // Now push a value into the Al, also held in packet buffer
        //
        byte[] myData = new byte[] {1};
        Proposal myValue = new Proposal();
        myValue.put("data", myData);
        myValue.put("handback", HANDBACK);

        myAl.messageReceived(
                new Begin(mySeqNum, myRndNum, myValue, _nodeId));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

        // Commit this instance
        //
        myAl.messageReceived(new Success(mySeqNum, myRndNum, _nodeId));

        CheckpointHandle mySecondHandle = myAl.newCheckpoint();
        
        Assert.assertTrue((mySecondHandle.isNewerThan(myFirstHandle)));

        // Execute and commit another instance
        //
        myAl.messageReceived(
                new Begin(mySeqNum + 1, myRndNum, myValue, _nodeId));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

        // Commit this instance
        //
        myAl.messageReceived(new Success(mySeqNum + 1, myRndNum, _nodeId));

        CheckpointHandle myThirdHandle = myAl.newCheckpoint();

        Assert.assertTrue((myThirdHandle.isNewerThan(mySecondHandle)));
        
        Assert.assertFalse(myFirstHandle.isNewerThan(myThirdHandle));
        
        Assert.assertFalse(mySecondHandle.isNewerThan(myThirdHandle));

        myAl.close();
    }
}

