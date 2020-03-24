package org.dancres.paxos.impl;

import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.utils.Builder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;

public class SimpleMembershipChangeTest {
    private Core _core1;
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    private FailureDetectorImpl _fd1;

    @Before
    public void init() throws Exception {
        Builder myBuilder = new Builder();

        _fd1 = new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN);
        _tport1 = myBuilder.newTransportWith(_fd1);
        _core1 = myBuilder.newCoreWith(_tport1);

        _tport2 = myBuilder.newDefaultStack();
    }

    @After
    public void stop() {
        _tport1.terminate();
        _tport2.terminate();
    }

    @Test
    public void post() throws Exception {
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        Assert.assertEquals(FailureDetectorImpl.OPEN_PIN, _fd1.getPinned());

        boolean myResult = _core1.updateMembership(_fd1.getMembers().getMembers().keySet());

        Assert.assertTrue(myResult);
        Assert.assertNotSame(FailureDetectorImpl.OPEN_PIN, _fd1.getPinned());
        Assert.assertEquals(2, _fd1.getPinned().size());

        for (InetSocketAddress myAddr : _fd1.getMembers().getMembers().keySet())
            Assert.assertTrue(_fd1.getPinned().contains(myAddr));
    }
}
