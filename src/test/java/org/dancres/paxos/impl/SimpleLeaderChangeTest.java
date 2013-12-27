package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

public class SimpleLeaderChangeTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    private FailureDetectorImpl _fd1;

    @Before
    public void init() throws Exception {
        _node1 = new ServerDispatcher();
        _node2 = new ServerDispatcher();
        _fd1 = new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN);
        _tport1 = new TransportImpl(_fd1);
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After
    public void stop() throws Exception {
        _tport1.terminate();
        _tport2.terminate();
    }

    @Test
    public void post() throws Exception {
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        Assert.assertEquals(FailureDetectorImpl.OPEN_PIN, _fd1.getPinned());

        boolean myResult = _node1.getCore().updateMembership(_fd1.getMembers().getMemberMap().keySet());

        Assert.assertTrue(myResult);
        Assert.assertEquals(2, _fd1.getPinned().size());

        for (InetSocketAddress myAddr : _fd1.getMembers().getMemberMap().keySet())
            Assert.assertTrue(_fd1.getPinned().contains(myAddr));
    }
}
