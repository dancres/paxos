package org.dancres.paxos.test.junit;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.impl.AcceptorLearner;
import org.dancres.paxos.impl.Stream;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.dancres.paxos.test.utils.Utils;
import org.dancres.paxos.test.utils.StandalonePickler;
import org.junit.Before;
import org.junit.Test;

public class ALStartupTest {
	private static final String DIRECTORY = "howllogs";

	private InetSocketAddress _nodeId = Utils.getTestAddress();
    private InetSocketAddress _broadcastId = Utils.getTestAddress();

	private class TransportImpl implements Transport {
        private Transport.PacketPickler _pickler = new StandalonePickler();

		private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

        public void add(Dispatcher aDispatcher) {
        }

		public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
			synchronized(_messages) {
				_messages.add(aMessage);
			}
		}		
		
        public Transport.PacketPickler getPickler() {
            return _pickler;
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

		public void connectTo(InetSocketAddress aNodeId, ConnectionHandler aHandler) {
		}
	}
		
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));    	
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl = new AcceptorLearner(myLogger, new Common(myTransport, new NullFailureDetector()));
        myAl.open(CheckpointHandle.NO_CHECKPOINT);
		myAl.close();
	}
	
	public static void main(String[] anArgs) throws Exception {
		ALStartupTest myTest = new ALStartupTest();
		myTest.init();
		myTest.test();
	}
}
