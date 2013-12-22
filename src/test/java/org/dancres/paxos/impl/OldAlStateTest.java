package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.*;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Last;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.net.*;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
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
	
	public static void main(String anArgs[]) throws Exception {
		OldAlStateTest myTest = new OldAlStateTest();
		myTest.init();
		myTest.test();
	}
	
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
	}
	
	private class TransportImpl implements Transport {
        private InetSocketAddress _broadcast = Utils.getTestAddress();
        private Transport.PacketPickler _pickler = new StandalonePickler(_nodeId);
        private MessageBasedFailureDetector _fd = new NullFailureDetector();

		private List<PaxosMessage> _messages = new ArrayList<>();

        public void routeTo(Dispatcher aDispatcher) {
        }

        public FailureDetector getFD() {
            return _fd;
        }

		public void send(Packet aPacket, InetSocketAddress aNodeId) {
			synchronized(_messages) {
				_messages.add(aPacket.getMessage());
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

        public Transport.PacketPickler getPickler() {
            return _pickler;
        }

        public InetSocketAddress getBroadcastAddress() {
            return _broadcast;
        }
		public InetSocketAddress getLocalAddress() {
			return _nodeId;
		}

		public void terminate() {
		}
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common(myTransport), new Listener() {
                    public void transition(StateEvent anEvent) {
                    }
                });

        myAl.open(CheckpointHandle.NO_CHECKPOINT);
		
		long myRndNum = 1;
		long mySeqNum = 0;
		
		// First collect, Al has no state so this is accepted
		//
		myAl.processMessage(new FakePacket(_nodeId, new Collect(mySeqNum, myRndNum)));
		
		PaxosMessage myResponse = myTransport.getNextMsg();	
		Assert.assertTrue(myResponse.getType() == Operations.LAST);
		
		// Now push a value into the Al
		//
		byte[] myData = new byte[] {1};
		Proposal myValue = new Proposal();
		myValue.put("data", myData);
		myValue.put("handback", HANDBACK);
		
		myAl.processMessage(new FakePacket(_nodeId,
                new Begin(mySeqNum, myRndNum, myValue)));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);

		/* 
		 * Emulate leader having to do recovery and re-run the paxos instance with a new rnd number - the response
		 * should be a last
		 */		
		myAl.processMessage(new FakePacket(_nodeId, new Collect(mySeqNum, myRndNum + 1)));
		
		Last myLast = (Last) myTransport.getNextMsg();
		
		Assert.assertTrue(myLast.getSeqNum() == mySeqNum);
		Assert.assertTrue(myLast.getRndNumber() == myRndNum);
		
		// Push the value again
		//
		myAl.processMessage(new FakePacket(_nodeId,
                new Begin(mySeqNum, myRndNum + 1, myLast.getConsolidatedValue())));
		
		myResponse = myTransport.getNextMsg();
		Assert.assertTrue(myResponse.getType() == Operations.ACCEPT);
		
		myAl.close();
	}	
}
