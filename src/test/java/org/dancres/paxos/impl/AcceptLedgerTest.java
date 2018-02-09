package org.dancres.paxos.impl;

import junit.framework.Assert;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.test.net.FakePacket;
import org.dancres.paxos.test.net.Utils;
import org.junit.Test;

public class AcceptLedgerTest {
    @Test
    public void duplicateLedger() {
        AcceptLedger myAL = new AcceptLedger("Test", 1);

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.add(new FakePacket(new Accept(1, 2)));
        myAL.add(new FakePacket(new Accept(1, 2)));
        myAL.add(new FakePacket(new Accept(1, 2)));

        Assert.assertNull(myAL.tally(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void majorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test", 1);
        Utils myUtils = new Utils();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.add(new FakePacket(Utils.getTestAddress(), new Accept(1, 2)));
        myAL.add(new FakePacket(Utils.getTestAddress(), new Accept(1, 2)));
        myAL.add(new FakePacket(Utils.getTestAddress(), new Accept(1, 2)));

        Assert.assertNotNull(myAL.tally(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void noMajorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test", 1);
        Utils myUtils = new Utils();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.add(new FakePacket(Utils.getTestAddress(), new Accept(1, 2)));
        myAL.add(new FakePacket(Utils.getTestAddress(), new Accept(1, 2)));

        Assert.assertNull(myAL.tally(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }
}
