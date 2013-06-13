package org.dancres.paxos.test.junit;

import java.io.File;
import java.nio.ByteBuffer;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HowlSequenceTest {
	private static final String _node1Log = "node1logs";
	private static final String _node2Log = "node2logs";
	
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(_node1Log));
    	FileSystem.deleteDirectory(new File(_node2Log));
    	
        Runtime.getRuntime().runFinalizersOnExit(true);

        _node1 = new ServerDispatcher(new FailureDetectorImpl(5000), new HowlLogger(_node1Log));
        _node2 = new ServerDispatcher(new FailureDetectorImpl(5000), new HowlLogger(_node2Log));
        _tport1 = new TransportImpl();
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl();
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }

    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        FDUtil.ensureFD(_node1.getCommon().getPrivateFD());
        FDUtil.ensureFD(_node2.getCommon().getPrivateFD());

        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);
            
            Proposal myProp = new Proposal("data", myBuffer.array());

            myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

            VoteOutcome myEv = myClient.getNext(10000);

            Assert.assertFalse((myEv == null));

            Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);

            Assert.assertTrue(myEv.getSeqNum() == i);
        }
        
        /*
         *  Let things settle before we close them off otherwise we can get a false assertion in the AL. This is
         *  because there are two nodes at play. Leader achieves success and posts this to both AL's, the first
         *  receives it and announces it to the test code which then completes and shuts things down before the
         *  second AL has finished processing the same success, by which time the log has been shut in that AL causing
         *  the assertion. 
         */
        Thread.sleep(5000);
        
        myTransport.terminate();
    }
}
