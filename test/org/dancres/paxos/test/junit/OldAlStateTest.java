package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.Stream;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Last;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.dancres.paxos.test.utils.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Objective of this test is to ensure the AL correctly remembers and recalls past Paxos rounds.
 */
public class OldAlStateTest {
	private static final String DIRECTORY = "howllogs";
	private static byte[] HANDBACK = new byte[] {1, 2, 3, 4};
	
	private InetSocketAddress _nodeId = Utils.getTestAddress();
	
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
	}
	
	private class TransportImpl implements Transport {
        private InetSocketAddress _broadcast = Utils.getTestAddress();

		private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
		
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

        public InetSocketAddress getBroadcastAddress() {
            return _broadcast;
        }
		public InetSocketAddress getLocalAddress() {
			return _nodeId;
		}

		public void shutdown() {
		}

		public Stream connectTo(InetSocketAddress aNodeId) {
			return null;
		}
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl = new AcceptorLearner(myLogger, new NullFailureDetector(), myTransport, 0);
        myAl.open();
		
		long myRndNum = 1;
		long mySeqNum = 0;
		
		// First collect, Al has no state so this is accepted
		//
		myAl.messageReceived(new Collect(mySeqNum, myRndNum, _nodeId));
		
		PaxosMessage myResponse = myTransport.getNextMsg();	
		Assert.assertTrue(myResponse.getType() == Operations.LAST);
		
		// Now push a value into the Al
		//
		byte[] myData = new byte[] {1};
		myAl.messageReceived(
				new Begin(mySeqNum, myRndNum, new ConsolidatedValue(myData, HANDBACK), _nodeId));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

		/* 
		 * Emulate leader having to do recovery and re-run the paxos instance with a new rnd number - the response
		 * should be a last
		 */		
		myAl.messageReceived(new Collect(mySeqNum, myRndNum + 1, _nodeId));
		
		Last myLast = (Last) myTransport.getNextMsg();
		
		Assert.assertTrue(myLast.getSeqNum() == mySeqNum);
		Assert.assertTrue(myLast.getRndNumber() == myRndNum);
		
		// Push the value again
		//
		myAl.messageReceived(
				new Begin(mySeqNum, myRndNum + 1, myLast.getConsolidatedValue(), _nodeId));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);
		
		myAl.close();
	}	
}
