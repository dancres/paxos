package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Last;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OldAlStateTest {
	private static final String DIRECTORY = "howllogs";
	private static byte[] HANDBACK = new byte[] {1, 2, 3, 4};
	
    private AddressGenerator _allocator;
    private InetSocketAddress _addr1;
	private NodeId _nodeId;
	
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
    	
        _allocator = new AddressGenerator();
        _addr1 = _allocator.allocate();
        _nodeId = NodeId.from(_addr1);
	}
	
	private static class TransportImpl implements Transport {
		private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
		
		public void send(PaxosMessage aMessage, NodeId aNodeId) {
			synchronized(_messages) {
				_messages.add(aMessage);
			}
		}		
		
		PaxosMessage getNextMsg() {
			synchronized(_messages) {
				return _messages.remove(0);
			}
		}
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl = new AcceptorLearner(myLogger, myTransport, _nodeId, 0);
		
		long myRndNum = 1;
		long mySeqNum = 0;
		
		// First collect, Al has no state so this is accepted and will be held in packet buffer
		//
		myAl.messageReceived(new Collect(mySeqNum, myRndNum, _nodeId.asLong()));
		
		PaxosMessage myResponse = myTransport.getNextMsg();	
		Assert.assertTrue(myResponse.getType() == Operations.LAST);
		
		// Now push a value into the Al, also held in packet buffer
		//
		byte[] myData = new byte[] {1};
		myAl.messageReceived(
				new Begin(mySeqNum, myRndNum, new ConsolidatedValue(myData, HANDBACK), _nodeId.asLong()));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

		/* 
		 * Emulate leader having to do recovery and re-run the paxos instance with a new rnd number - the response
		 * should be a last and it will be sourced from the packet buffer.
		 */		
		myAl.messageReceived(new Collect(mySeqNum, myRndNum + 1, _nodeId.asLong()));
		
		Last myLast = (Last) myTransport.getNextMsg();
		
		Assert.assertTrue(myLast.getSeqNum() == mySeqNum);
		Assert.assertTrue(myLast.getRndNumber() == myRndNum);
		
		// Push the value again
		//
		myAl.messageReceived(
				new Begin(mySeqNum, myRndNum + 1, myLast.getConsolidatedValue(), _nodeId.asLong()));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);
		
		// Commit this instance
		//
		myAl.messageReceived(new Success(mySeqNum, myRndNum + 1, myLast.getConsolidatedValue(), _nodeId.asLong()));

		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACK);
		
		/*
		 *  Now re-run the instance again, results should be sourced from the log file and have the last
		 *  round number used.
		 */		
		myAl.messageReceived(new Collect(mySeqNum, myRndNum + 2, _nodeId.asLong()));
		
		myLast = (Last) myTransport.getNextMsg();
		
		Assert.assertTrue(myLast.getSeqNum() == mySeqNum);
		Assert.assertTrue(myLast.getRndNumber() == myRndNum + 1);
		
		myAl.close();
	}	
}
