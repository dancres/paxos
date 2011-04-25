package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.Stream;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.dancres.paxos.test.utils.Utils;
import org.junit.Before;
import org.junit.Test;

public class ALStartupTest {
	private static final String DIRECTORY = "howllogs";

	private InetSocketAddress _nodeId = Utils.getTestAddress();
    private InetSocketAddress _broadcastId = Utils.getTestAddress();

	private class TransportImpl implements Transport {
		private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
		
		public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
			synchronized(_messages) {
				_messages.add(aMessage);
			}
		}		
		
		PaxosMessage getNextMsg() {
			synchronized(_messages) {
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

		public Stream connectTo(InetSocketAddress aNodeId) {
			return null;
		}	
	}
		
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));    	
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl = new AcceptorLearner(myLogger, new NullFailureDetector(), myTransport, 0);
        myAl.open();
		myAl.close();
	}
	
	public static void main(String[] anArgs) throws Exception {
		ALStartupTest myTest = new ALStartupTest();
		myTest.init();
		myTest.test();
	}
}
