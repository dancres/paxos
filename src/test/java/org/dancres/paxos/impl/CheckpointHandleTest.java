package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.AcceptorLearner;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Learned;
import org.dancres.paxos.test.net.*;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
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
        private Transport.PacketPickler _pickler = new StandalonePickler();

        private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
        private InetSocketAddress _nodeId;
        private InetSocketAddress _broadcastId;

        TransportImpl(InetSocketAddress aNodeId, InetSocketAddress aBroadcastId) {
            _nodeId = aNodeId;
            _broadcastId = aBroadcastId;
        }

        public Transport.PacketPickler getPickler() {
            return _pickler;
        }

        public void routeTo(Dispatcher aDispatcher) {
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

        public void terminate() {
        }

        public void connectTo(InetSocketAddress aNodeId, ConnectionHandler aHandler) {
        }
    }

    @Test
    public void test() throws Exception {
        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        TransportImpl myTransport = new TransportImpl(_nodeId, _broadcastId);
        Common myCommon = new Common(myTransport, new NullFailureDetector());

        AcceptorLearner myAl = new AcceptorLearner(myLogger, myCommon, new Listener() {
            public void transition(StateEvent anEvent) {
            }
        });
        myAl.open(CheckpointHandle.NO_CHECKPOINT);

        Assert.assertFalse(myCommon.testState(Constants.FSMStates.RECOVERING));

        long myRndNum = 1;
        long mySeqNum = 0;

        CheckpointHandle myFirstHandle = myAl.newCheckpoint();
        
        // First collect, Al has no state so this is accepted and will be held in packet buffer
        //
        myAl.processMessage(new FakePacket(_nodeId, new Collect(mySeqNum, myRndNum)));

        PaxosMessage myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.LAST);

        // Now push a value into the Al, also held in packet buffer
        //
        byte[] myData = new byte[] {1};
        Proposal myValue = new Proposal();
        myValue.put("data", myData);
        myValue.put("handback", HANDBACK);

        myAl.processMessage(new FakePacket(_nodeId,
                new Begin(mySeqNum, myRndNum, myValue)));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

        // Commit this instance
        //
        myAl.processMessage(new FakePacket(_nodeId, new Learned(mySeqNum, myRndNum)));

        CheckpointHandle mySecondHandle = myAl.newCheckpoint();
        
        Assert.assertTrue((mySecondHandle.isNewerThan(myFirstHandle)));

        // Execute and commit another instance
        //
        myAl.processMessage(new FakePacket(_nodeId,
                new Begin(mySeqNum + 1, myRndNum, myValue)));

        myResponse = myTransport.getNextMsg();
        Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

        // Commit this instance
        //
        myAl.processMessage(new FakePacket(_nodeId, new Learned(mySeqNum + 1, myRndNum)));

        CheckpointHandle myThirdHandle = myAl.newCheckpoint();

        Assert.assertTrue((myThirdHandle.isNewerThan(mySecondHandle)));
        
        Assert.assertFalse(myFirstHandle.isNewerThan(myThirdHandle));
        
        Assert.assertFalse(mySecondHandle.isNewerThan(myThirdHandle));

        myAl.close();
    }
}
