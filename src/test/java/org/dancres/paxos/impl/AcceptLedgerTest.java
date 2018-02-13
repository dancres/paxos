package org.dancres.paxos.impl;

import junit.framework.Assert;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.test.net.StandalonePickler;
import org.dancres.paxos.test.net.TestAddresses;
import org.junit.Test;

import java.net.InetSocketAddress;

public class AcceptLedgerTest {
    @Test
    public void duplicateLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");
        StandalonePickler myPickler =
                new StandalonePickler(new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 12345));

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2)));
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2)));
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2)));

        Assert.assertNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void majorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");
        StandalonePickler myPickler = new StandalonePickler();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2), TestAddresses.next()));
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2), TestAddresses.next()));
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2), TestAddresses.next()));

        Assert.assertNotNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }

    @Test
    public void noMajorityLedger() {
        AcceptLedger myAL = new AcceptLedger("Test");
        StandalonePickler myPickler = new StandalonePickler();

        // Although there are 3 packets, they are all from the same source so cannot constitute a majority
        //
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2), TestAddresses.next()));
        myAL.extendLedger(myPickler.newPacket(new Accept(1, 2), TestAddresses.next()));

        Assert.assertNull(myAL.tallyAccepts(new Begin(1, 2, new Proposal("key", new byte[0])), 3));
    }
}
