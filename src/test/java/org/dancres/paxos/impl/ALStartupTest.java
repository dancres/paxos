package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.net.Utils;
import org.dancres.paxos.test.net.StandalonePickler;
import org.junit.Before;
import org.junit.Test;

public class ALStartupTest {
	private static final String DIRECTORY = "howllogs";

	private InetSocketAddress _nodeId = Utils.getTestAddress();
    private InetSocketAddress _broadcastId = Utils.getTestAddress();

	private class TransportImpl implements Transport {
        private Transport.PacketPickler _pickler = new StandalonePickler(_nodeId);
        private MessageBasedFailureDetector _fd = new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN);

		private List<PaxosMessage> _messages = new ArrayList<>();

		public void send(Packet aPacket, InetSocketAddress aNodeId) {
			synchronized(_messages) {
				_messages.add(aPacket.getMessage());
			}
		}		
		
        public Transport.PacketPickler getPickler() {
            return _pickler;
        }

        public FailureDetector getFD() {
            return _fd;
        }

        public void routeTo(Dispatcher aDispatcher) throws Exception {
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

        public void terminate() {
		}
	}
		
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));    	
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		TransportImpl myTransport = new TransportImpl();
		
		AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common(myTransport));
        myAl.open(CheckpointHandle.NO_CHECKPOINT);
		myAl.close();
	}
	
	public static void main(String[] anArgs) throws Exception {
		ALStartupTest myTest = new ALStartupTest();
		myTest.init();
		myTest.test();
	}
}
