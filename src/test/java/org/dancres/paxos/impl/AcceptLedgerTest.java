package org.dancres.paxos.impl;

import junit.framework.Assert;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.test.net.FakePacket;
import org.junit.Test;

public class AcceptLedgerTest {
    @Test
    public void breakLedger() throws Exception {
        AcceptLedger myAL = new AcceptLedger("Test", 1);

        myAL.add(new FakePacket(new Accept(1, 2)));
        myAL.add(new FakePacket(new Accept(1, 2)));
        myAL.add(new FakePacket(new Accept(1, 2)));

        Assert.assertNull(myAL.tally(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }
}
