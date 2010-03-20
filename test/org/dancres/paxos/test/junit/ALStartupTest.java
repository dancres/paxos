package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.junit.Before;
import org.junit.Test;

public class ALStartupTest {
	private static final String DIRECTORY = "howllogs";

    private AddressGenerator _allocator;
    private InetSocketAddress _addr1;
	private NodeId _nodeId;
	
	private class TransportImpl implements Transport {
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

		public NodeId getLocalNodeId() {
			return _nodeId;
		}
	}
		
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
    	
        _allocator = new AddressGenerator();
        _addr1 = _allocator.allocate();
        _nodeId = NodeId.from(_addr1);
	}
	
	@Test public void test() {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl = new AcceptorLearner(myLogger, new NullFailureDetector(), myTransport, 0);
		myAl.close();
	}
	
	public static void main(String[] anArgs) throws Exception {
		ALStartupTest myTest = new ALStartupTest();
		myTest.init();
		myTest.test();
	}
}
