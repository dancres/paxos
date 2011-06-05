package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;

public class FDTest implements MembershipListener {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher(5000, "node1".getBytes());
    	_node2 = new ServerDispatcher(5000, "node2".getBytes());
        _tport1 = new TransportImpl();
        _tport1.add(_node1);
        _tport2 = new TransportImpl();
        _tport2.add(_node2);
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
        ensureFD(_node1.getFailureDetector());
        ensureFD(_node2.getFailureDetector());

        assert(_node1.getFailureDetector().getMemberSet().size() == 2);
        assert(_node2.getFailureDetector().getMemberSet().size() == 2);

        Set<InetSocketAddress> myMembers = _node1.getFailureDetector().getMemberSet();
        Iterator<InetSocketAddress> myMemberIt = myMembers.iterator();

        while(myMemberIt.hasNext()) {
            InetSocketAddress myAddr = myMemberIt.next();
            byte[] myMeta = _node1.getFailureDetector().getMetaData(myAddr);

            if (myAddr.equals(_tport1.getLocalAddress())) {
                assert("node1".equals(new String(myMeta)));
            } else if (myAddr.equals(_tport2.getLocalAddress())) {
                assert("node2".equals(new String(myMeta)));                
            } else {
                Assert.fail();
            }
        }
    }

    public void abort() {
    }

    public void allReceived() {
    }
}
