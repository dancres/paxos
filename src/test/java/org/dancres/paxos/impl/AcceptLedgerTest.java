package org.dancres.paxos.impl;

import junit.framework.Assert;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.test.net.FakePacket;
import org.dancres.paxos.test.net.TestAddresses;
import org.junit.Test;

public class AcceptLedgerTest {
    @Test
    public void duplicateLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(new FakePacket(new Accept(1, 2)));
        myAL.extendLedger(new FakePacket(new Accept(1, 2)));
        myAL.extendLedger(new FakePacket(new Accept(1, 2)));

        Assert.assertNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void majorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");
        TestAddresses myUtils = new TestAddresses();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(new FakePacket(TestAddresses.next(), new Accept(1, 2)));
        myAL.extendLedger(new FakePacket(TestAddresses.next(), new Accept(1, 2)));
        myAL.extendLedger(new FakePacket(TestAddresses.next(), new Accept(1, 2)));

        Assert.assertNotNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void noMajorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");
        TestAddresses myUtils = new TestAddresses();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(new FakePacket(TestAddresses.next(), new Accept(1, 2)));
        myAL.extendLedger(new FakePacket(TestAddresses.next(), new Accept(1, 2)));

        Assert.assertNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }
}
