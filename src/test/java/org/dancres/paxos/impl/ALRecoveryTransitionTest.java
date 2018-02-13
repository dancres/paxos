package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Future;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.netty.PicklerImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Need;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Learned;
import org.dancres.paxos.test.net.*;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ALRecoveryTransitionTest {
	private static final String DIRECTORY = "howllogs";
	private static byte[] HANDBACK = new byte[] {1, 2, 3, 4};
	
	private static InetSocketAddress _nodeId = TestAddresses.next();
    private static InetSocketAddress _broadcastId = TestAddresses.next();

	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
	}

    private static class FakeDetector extends MessageBasedFailureDetector {
        public Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData) {
            return null;
        }

        public void stop() {
        }

        public void addListener(StateListener aListener) {
        }

        public InetSocketAddress getRandomMember(InetSocketAddress aLocal) {
            return _nodeId;
        }

        public boolean isMember(InetSocketAddress anAddress) {
            return true;
        }

        public Assembly getMembers() {
            return null;
        }

        public byte[] dataForNode(InetSocketAddress anAddress) {
            return new byte[0];
        }

        public int getMajority() {
            return 2;
        }

        public Future<Assembly> barrier() {
            return null;
        }

        public Future<Assembly> barrier(int aRequired) {
            return null;
        }

        public void pin(Collection<InetSocketAddress> aMembers) {
        }

        public boolean accepts(Transport.Packet aPacket) {
            return aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.FAILURE_DETECTOR);
        }

        public void processMessage(Transport.Packet aPacket) {
        }
    }

	@Test public void test() throws Exception {
        PicklerImpl myPickler = new PicklerImpl();
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		ALTestTransportImpl myTransport = new ALTestTransportImpl(_nodeId, _broadcastId, new FakeDetector());
        Common myCommon = new Common().setTransport(myTransport);

		AcceptorLearner myAl = new AcceptorLearner(myLogger, myCommon);

        myAl.open(CheckpointHandle.NO_CHECKPOINT);
		
		Assert.assertFalse(myCommon.getNodeState().test(NodeState.State.RECOVERING));
		
		long myRndNum = 1;
		long mySeqNum = 0;
		
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

		// Now start an instance which should trigger recovery - happens on collect boundary
		//
		myAl.processMessage(myPickler.newPacket(new Collect(mySeqNum + 5, myRndNum + 2), _nodeId));
		
		Assert.assertTrue(myCommon.getNodeState().test(NodeState.State.RECOVERING));
		
		/*
		 * Recovery range r is lwm < r <= x - 1 (where x = tooNewCollect.seqNum)
		 * lwm after one successful round should 0. 
		 */
		Need myNeed = (Need) myTransport.getNextMsg();
		
		Assert.assertEquals(myNeed.getMinSeq(), 0);
		Assert.assertEquals(myNeed.getMaxSeq(), mySeqNum + 4);
		
		myAl.close();
	}
}
