package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Membership;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class FDTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher();
    	_node2 = new ServerDispatcher();
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000), "node1".getBytes());
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000), "node2".getBytes());
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }

    @Test public void post() throws Exception {
        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        Assert.assertTrue(_tport1.getFD().getMembers().getMemberMap().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMemberMap().size() == 2);

        Map<InetSocketAddress, FailureDetector.MetaData> myMembers = _tport1.getFD().getMembers().getMemberMap();

        for (Map.Entry<InetSocketAddress, FailureDetector.MetaData> myEntry : myMembers.entrySet()) {
            if (myEntry.getKey().equals(_tport1.getLocalAddress())) {
                Assert.assertTrue("node1".equals(new String(myEntry.getValue().getData())));
            } else if (myEntry.getKey().equals(_tport2.getLocalAddress())) {
                Assert.assertTrue("node2".equals(new String(myEntry.getValue().getData())));
            } else {
                Assert.fail();
            }
        }
    }

    @Test public void pin() throws Exception {
        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        Assert.assertTrue(_tport1.getFD().getMembers().getMemberMap().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMemberMap().size() == 2);

        Collection<InetSocketAddress> myMembers = _tport1.getFD().getMembers().getMemberMap().keySet();

        _tport1.getFD().pin(myMembers);
        _tport2.getFD().pin(myMembers);

        TransportImpl myTport = new TransportImpl(new FailureDetectorImpl(5000), "node3".getBytes());

        // Should work just fine if it exceptions or breaks the FD underneath, we'll know as we proceed
        //
        myTport.getFD().pin(null);

        // myTport should end up with an FD containing 3 nodes because it is unpinned
        //
        Membership myMembership = myTport.getFD().barrier(3).get(10000, TimeUnit.MILLISECONDS);

        Assert.assertNotNull(myMembership);

        // Both other transports should still have membership of 2 and not mention node3
        //
        Assert.assertTrue(_tport1.getFD().getMembers().getMemberMap().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMemberMap().size() == 2);

        Assert.assertFalse(_tport1.getFD().getMembers().getMemberMap().containsValue("node3"));
        Assert.assertFalse(_tport2.getFD().getMembers().getMemberMap().containsValue("node3"));

        myTport.terminate();
    }
}
