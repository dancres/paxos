package org.dancres.paxos.impl;

import junit.framework.Assert;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;

import org.dancres.paxos.bus.Messages;
import org.dancres.paxos.bus.MessagesImpl;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ProposalAllocatorTest {
    private class Listener implements Messages.Subscriber<Constants.EVENTS> {
        private AtomicInteger _inflightCount = new AtomicInteger();
        private AtomicInteger _allConcludedCount = new AtomicInteger();

        void inFlight() {
            int myCurrent = _inflightCount.incrementAndGet();

            Assert.assertTrue(myCurrent > _allConcludedCount.get());
        }

        void allConcluded() {
            int myCurrent = _allConcludedCount.incrementAndGet();

            Assert.assertTrue(myCurrent == _inflightCount.get());
        }

        int getInFlight() {
            return _inflightCount.get();
        }

        int getAllConcluded() {
            return _allConcludedCount.get();
        }

        @Override
        public void msg(Messages.Message<Constants.EVENTS> aMessage) {
            switch(aMessage.getType()) {
                case PROP_ALLOC_ALL_CONCLUDED : allConcluded(); break;
                case PROP_ALLOC_INFLIGHT: inFlight(); break;
            }
        }
    }

    @Test
    public void listener() {
        Messages<Constants.EVENTS> myBus = new MessagesImpl<>();
        int myMaxInflight = 5;

        ProposalAllocator myFactory = new ProposalAllocator(myBus, myMaxInflight).resumeAt(-1, 0);
        Listener myListener = new Listener();

        myBus.subscribe("listener", myListener);

        Instance myInstance = myFactory.nextInstance(1);

        Assert.assertEquals(1, myListener.getInFlight());

        myFactory.conclusion(myInstance,
                new VoteOutcome(VoteOutcome.Reason.VALUE, myInstance.getSeqNum(), myInstance.getRound(),
                        Proposal.NO_VALUE, null));

        Assert.assertEquals(1, myListener.getAllConcluded());

        List<Instance> myInstances = new LinkedList<>();

        for (int i = 0; i < (myMaxInflight - 1); i++) {
            myInstances.add(myFactory.nextInstance(1));

            if (i == 0)
                Assert.assertEquals(2, myListener.getInFlight());
        }

        for (Instance myI : myInstances)
            myFactory.conclusion(myI,
                    new VoteOutcome(VoteOutcome.Reason.VALUE, myI.getSeqNum(), myI.getRound(),
                            Proposal.NO_VALUE, null));

        Assert.assertEquals(2, myListener.getAllConcluded());
    }

    @Test
    public void oneLeader() {
        Messages<Constants.EVENTS> myBus = new MessagesImpl<>();
        int myMaxInflight = 5;

        ProposalAllocator myFactory = new ProposalAllocator(myBus, myMaxInflight).resumeAt(-1, 0);

        // Not yet a leader, so max of one in-flight instance applies (or should)
        Instance myFirstInstance = myFactory.nextInstance(1);

        Instance mySecondInstance = myFactory.nextInstance(1);

        Assert.assertNull(mySecondInstance);

        myFactory.conclusion(myFirstInstance,
                new VoteOutcome(VoteOutcome.Reason.VALUE, myFirstInstance.getSeqNum(), myFirstInstance.getRound(),
                        Proposal.NO_VALUE, null));

        int myCount = 0;

        while ((myFactory.nextInstance(1) != null) && (myCount <= myMaxInflight))
            myCount++;

        Assert.assertNull(myFactory.nextInstance(1));
        Assert.assertEquals(myMaxInflight, myCount);
    }

    @Test
    public void correctSequence() {
        Messages<Constants.EVENTS> myBus = new MessagesImpl<>();
        ProposalAllocator myFactory = new ProposalAllocator(myBus).resumeAt(-1, 0);

        for (int i = 0; i < 10; i++) {
            Instance myInstance = myFactory.nextInstance(1);

            Assert.assertEquals(i, myInstance.getSeqNum());

            myFactory.conclusion(myInstance,
                    new VoteOutcome(VoteOutcome.Reason.VALUE, myInstance.getSeqNum(), myInstance.getRound(),
                            Proposal.NO_VALUE, null));
        }

        for (int i = 10; i < 20; i++) {
            Instance myInstance = myFactory.nextInstance(1);

            Assert.assertEquals(i, myInstance.getSeqNum());

            myFactory.conclusion(myInstance,
                    new VoteOutcome(VoteOutcome.Reason.VALUE, myInstance.getSeqNum(), myInstance.getRound(),
                            Proposal.NO_VALUE, null));
        }
    }

    @Test
    public void reuseSequenceOnFail() {
        Messages<Constants.EVENTS> myBus = new MessagesImpl<>();
        ProposalAllocator myFactory = new ProposalAllocator(myBus).resumeAt(-1, 0);
        Instance myInstance = myFactory.nextInstance(1);

        Assert.assertNotNull(myInstance);
        Assert.assertEquals(0, myInstance.getSeqNum());

        myFactory.conclusion(myInstance,
                new VoteOutcome(VoteOutcome.Reason.VOTE_TIMEOUT, myInstance.getSeqNum(), myInstance.getRound(),
                        Proposal.NO_VALUE, null));

        myInstance = myFactory.nextInstance(1);

        Assert.assertNotNull(myInstance);
        Assert.assertEquals(0, myInstance.getSeqNum());

        myFactory.conclusion(myInstance,
                new VoteOutcome(VoteOutcome.Reason.BAD_MEMBERSHIP, myInstance.getSeqNum(), myInstance.getRound(),
                        Proposal.NO_VALUE, null));

        myInstance = myFactory.nextInstance(1);

        Assert.assertNotNull(myInstance);
        Assert.assertEquals(0, myInstance.getSeqNum());
    }

    @Test
    public void reuseSomeOnOtherLeader() {
        Messages<Constants.EVENTS> myBus = new MessagesImpl<>();

        // This test can't run if inflight is too small
        //
        int myMaxInflight = 5;

        ProposalAllocator myFactory = new ProposalAllocator(myBus, myMaxInflight).resumeAt(-1, 0);
        Instance myInstance = myFactory.nextInstance(1);

        Assert.assertNotNull(myInstance);
        Assert.assertEquals(0, myInstance.getSeqNum());

        myFactory.conclusion(myInstance,
                new VoteOutcome(VoteOutcome.Reason.VALUE, myInstance.getSeqNum(), myInstance.getRound(),
                        Proposal.NO_VALUE, null));

        List<Instance> myInstances = new LinkedList<>();

        for (int i = 0; i < (myMaxInflight - 1); i++) {
            myInstances.add(myFactory.nextInstance(1));
        }

        // Inject a retryable failure so we can ensure reservations are vanquished
        //
        Instance myFailedInstance = myInstances.get(2);
        myFactory.conclusion(myFailedInstance,
                new VoteOutcome(VoteOutcome.Reason.VOTE_TIMEOUT, myFailedInstance.getSeqNum(),
                        myFailedInstance.getRound(), Proposal.NO_VALUE, null));

        Instance mySplitInstance = myInstances.get(myInstances.size() - 1);
        myFactory.conclusion(mySplitInstance,
                new VoteOutcome(VoteOutcome.Reason.OTHER_LEADER, mySplitInstance.getSeqNum(),
                        mySplitInstance.getRound(), Proposal.NO_VALUE, null));

        myInstance = myFactory.nextInstance(1);

        Assert.assertEquals(mySplitInstance.getSeqNum() + 1, myInstance.getSeqNum());
        Assert.assertEquals(2, myInstance.getRound());
    }
}
