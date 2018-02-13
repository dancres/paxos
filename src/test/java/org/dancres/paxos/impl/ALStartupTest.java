package org.dancres.paxos.impl;

import java.io.File;
import java.net.InetSocketAddress;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.net.TestAddresses;
import org.junit.Before;
import org.junit.Test;

public class ALStartupTest {
	private static final String DIRECTORY = "howllogs";

	private InetSocketAddress _nodeId = TestAddresses.next();
    private InetSocketAddress _broadcastId = TestAddresses.next();

	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));    	
	}
	
	@Test public void test() throws Exception {
		HowlLogger myLogger = new HowlLogger(DIRECTORY);
		ALTestTransportImpl myTransport = new ALTestTransportImpl(_nodeId, _broadcastId,
				new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
		
		AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common().setTransport(myTransport));
        myAl.open(CheckpointHandle.NO_CHECKPOINT);
		myAl.close();
	}
	
	public static void main(String[] anArgs) throws Exception {
		ALStartupTest myTest = new ALStartupTest();
		myTest.init();
		myTest.test();
	}
}
