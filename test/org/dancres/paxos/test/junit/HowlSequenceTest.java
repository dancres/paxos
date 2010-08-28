package org.dancres.paxos.test.junit;

import java.io.File;
import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.test.utils.ClientDispatcher;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.ServerDispatcher;
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

        _node1 = new ServerDispatcher(5000, new HowlLogger(_node1Log));
        _node2 = new ServerDispatcher(5000, new HowlLogger(_node2Log));        
        _tport1 = new TransportImpl(_node1);
        _tport2 = new TransportImpl(_node2);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    	_node2.stop();
    }

    private void ensureFD(FailureDetector anFD) throws Exception {
        int myChances = 0;

        while (!anFD.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }
    }


    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(myClient);

        ensureFD(_node1.getFailureDetector());
        ensureFD(_node2.getFailureDetector());

        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);

            myClient.send(new Post(myBuffer.array(), myTransport.getLocalAddress()),
            	_tport2.getLocalAddress());

            PaxosMessage myMsg = myClient.getNext(10000);

            Assert.assertFalse((myMsg == null));

            System.err.println("Got message: " + myMsg.getType());

            Assert.assertTrue(myMsg.getType() == Operations.COMPLETE);

            Assert.assertTrue(myMsg.getSeqNum() == i);
        }
        
        /*
         *  Let things settle before we close them off otherwise we can get a false assertion in the AL. This is
         *  because there are two nodes at play. Leader achieves success and posts this to both AL's, the first
         *  receives it and announces it to the test code which then completes and shuts things down before the
         *  second AL has finished processing the same success, by which time the log has been shut in that AL causing
         *  the assertion. 
         */
        Thread.sleep(5000);
        
        myClient.shutdown();
    }
}
