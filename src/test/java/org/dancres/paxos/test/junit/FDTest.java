package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.MembershipListener;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

public class FDTest implements MembershipListener {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher(new FailureDetectorImpl(5000), "node1".getBytes());
    	_node2 = new ServerDispatcher(new FailureDetectorImpl(5000), "node2".getBytes());
        _tport1 = new TransportImpl();
        _tport1.add(_node1);
        _tport2 = new TransportImpl();
        _tport2.add(_node2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }

    private void ensureFD(MessageBasedFailureDetector anFD) throws Exception {
        int myChances = 0;

        while (!anFD.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }
    }

    @Test public void post() throws Exception {
        ensureFD(_node1.getCommon().getPrivateFD());
        ensureFD(_node2.getCommon().getPrivateFD());

        Assert.assertTrue(_node1.getCommon().getFD().getMemberMap().size() == 2);
        Assert.assertTrue(_node2.getCommon().getFD().getMemberMap().size() == 2);

        Map<InetSocketAddress, FailureDetector.MetaData> myMembers = _node1.getCommon().getFD().getMemberMap();
        Iterator<Map.Entry<InetSocketAddress, FailureDetector.MetaData>> myMemberIt = myMembers.entrySet().iterator();

        while(myMemberIt.hasNext()) {
            Map.Entry<InetSocketAddress, FailureDetector.MetaData> myEntry = myMemberIt.next();

            if (myEntry.getKey().equals(_tport1.getLocalAddress())) {
                Assert.assertTrue("node1".equals(new String(myEntry.getValue().getData())));
            } else if (myEntry.getKey().equals(_tport2.getLocalAddress())) {
                Assert.assertTrue("node2".equals(new String(myEntry.getValue().getData())));
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
