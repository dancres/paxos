package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.Event;
import org.junit.*;

public class NoMajorityTest {
    private ServerDispatcher _node1;

    private TransportImpl _tport1;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(5000);
        _tport1 = new TransportImpl();
        _tport1.add(_node1);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.add(myClient);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Thread.sleep(5000);

        myClient.send(new Post(myBuffer.array(), myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        PaxosMessage myMsg = myClient.getNext(10000);

        Assert.assertFalse((myMsg == null));

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);

        Fail myFail = (Fail) myMsg;

        Assert.assertTrue(myFail.getReason() == Event.Reason.BAD_MEMBERSHIP);
    }
}
