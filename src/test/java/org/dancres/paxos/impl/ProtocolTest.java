package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Claim;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.test.net.TestAddresses;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class ProtocolTest {
    @Test
    public void sanity() {
        Assert.assertEquals("Critical condition for tests breached [SEQ]",
                Constants.PRIMORDIAL_SEQ, -1);
        Assert.assertEquals("Critical condition for tests breached [RND]",
                Constants.PRIMORDIAL_RND, Long.MIN_VALUE);
    }

    @Test
    public void validateWatermarkChecks() {
        Collect myBefore = new Collect(4, Constants.PRIMORDIAL_RND);
        Collect myAfter = new Collect(6, Constants.PRIMORDIAL_RND);
        Watermark myPointInTime = new Watermark(5, -1);

        Assert.assertTrue(Consensus.isOldSeq.apply(myBefore, myPointInTime));
        Assert.assertFalse(Consensus.isOldSeq.apply(myAfter, myPointInTime));
    }

    @Test
    public void validateRndChecks() {
        Collect myPast = new Collect(Constants.PRIMORDIAL_SEQ, 4);
        Collect myElected = new Collect(Constants.PRIMORDIAL_SEQ, 6);

        Assert.assertTrue(Consensus.isPastRnd.apply(myPast, myElected));
        Assert.assertFalse(Consensus.isPastRnd.apply(myElected, myPast));

        Collect myElectable = new Collect(Constants.PRIMORDIAL_SEQ, 8);

        Assert.assertTrue(Consensus.isElectableRnd.apply(myElectable, myElected));
        Assert.assertFalse(Consensus.isElectableRnd.apply(myPast, myElected));

        Begin mySameRnd = new Begin(Constants.PRIMORDIAL_SEQ, myElected.getRndNumber(), Proposal.NO_VALUE);
        Assert.assertTrue(Consensus.isTheElectedRnd.apply(mySameRnd, myElected));

        Assert.assertTrue(Consensus.shouldWriteCollect.apply(myElectable, myElected));
        Assert.assertFalse(Consensus.shouldWriteCollect.apply(myPast, myElected));
    }

    @Test
    public void validateCollectOld() {
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());

        // Old round
        Assert.assertTrue(myMachine.old(new Collect(5, 10),
                new Collect(5, 11),
                new Watermark(5, -1)));

        // Not electable
        Assert.assertTrue(myMachine.old(new Collect(5, 11),
                new Collect(5, 11),
                new Watermark(5, -1)));

        // Old sequence number
        Assert.assertTrue(myMachine.old(new Collect(5, 10),
                new Collect(5, 9),
                new Watermark(6, -1)));
    }

    @Test
    public void validateCollectActionable() {
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());
        InetSocketAddress myAddrOne = TestAddresses.next();
        InetSocketAddress myAddrTwo = TestAddresses.next();

        Assert.assertTrue("Same leader and greater round",
                myMachine.actionable(new Collect(Constants.PRIMORDIAL_SEQ, 20),
                myAddrOne, new Collect(Constants.PRIMORDIAL_SEQ, 19),
                myAddrOne, 0));

        Assert.assertTrue("Different leader but expired",
                myMachine.actionable(new Collect(Constants.PRIMORDIAL_SEQ, 20),
                myAddrOne, new Collect(Constants.PRIMORDIAL_SEQ, 19),
                myAddrTwo, 0));

        Assert.assertFalse("Different leader but not expired",
                myMachine.actionable(new Collect(Constants.PRIMORDIAL_SEQ, 20),
                myAddrOne, new Collect(Constants.PRIMORDIAL_SEQ, 19),
                myAddrTwo, System.currentTimeMillis() + Leader.LeaseDuration.get() + 60000));

        Assert.assertFalse("Different leader, expired but not electable",
                myMachine.actionable(new Collect(Constants.PRIMORDIAL_SEQ, 19),
                myAddrOne, new Collect(Constants.PRIMORDIAL_SEQ, 19),
                myAddrTwo, 0));
    }

    @Test
    public void validateBeginOld() {
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());

        Assert.assertTrue("Begin is for old round but newer sequence number",
                myMachine.old(new Begin(2, 5, Proposal.NO_VALUE),
                new Collect(2, 6), new Watermark(1, -1)));

        Assert.assertTrue("Begin is for current round but older sequence number",
                myMachine.old(new Begin(1, 6, Proposal.NO_VALUE),
                        new Collect(2, 6), new Watermark(2, -1)));

        Assert.assertFalse("Begin is for current round and newer sequence number",
                myMachine.old(new Begin(3, 6, Proposal.NO_VALUE),
                        new Collect(2, 6), new Watermark(2, -1)));
    }

    @Test
    public void validateBeginActionable() {
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());

        Assert.assertFalse("Not from elected, too low",
                myMachine.actionable(new Begin(5, 4, Proposal.NO_VALUE),
                new Collect(5, 5)));

        Assert.assertFalse("Not from elected, too high",
                myMachine.actionable(new Begin(5, 10, Proposal.NO_VALUE),
                        new Collect(5, 5)));

        Assert.assertTrue("From elected",
                myMachine.actionable(new Begin(5, 10, Proposal.NO_VALUE),
                        new Collect(5, 10)));
    }

    static class ActCapture implements Consensus.Act {
        boolean _acted = false;

        private final Claim _successful;

        ActCapture(Claim aSuccess) {
            _successful = aSuccess;
        }

        @Override
        public void accept(Claim aClaim, Boolean aWriteFlag) {
            Assert.assertTrue(aWriteFlag);
            Assert.assertEquals(_successful, aClaim);
            _acted = true;
        }
    }

    static class OutdatedCapture implements Consensus.Outdated {
        boolean _outdated = false;

        private final InetSocketAddress _proposer;

        OutdatedCapture(InetSocketAddress aProposer) {
            _proposer = aProposer;
        }

        @Override
        public void accept(Long aLong, InetSocketAddress anAddress) {
            Assert.assertTrue(Collect.INITIAL.getRndNumber() == aLong);
            Assert.assertNotSame(_proposer, anAddress);
            _outdated = true;
        }
    }

    @Test
    public void checkCollect() {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());
        InetSocketAddress myProposer = TestAddresses.next();
        long myPreviousExpiry = myMachine.getExpiry();

        OutdatedCapture myOutdatedCapture = new OutdatedCapture(myProposer);

        myMachine.dispatch(new Collect(Constants.PRIMORDIAL_SEQ, 5),
                myProposer, new Watermark(Constants.PRIMORDIAL_SEQ, -1),
                myOutdatedCapture,
                (c, w) -> {});

        Assert.assertTrue("Primordial collect cannot supersede same", myOutdatedCapture._outdated);
        Assert.assertEquals(myPreviousExpiry, myMachine.getExpiry());
        Assert.assertEquals(Collect.INITIAL, myMachine.getElected());
        Assert.assertNotSame(myProposer, myMachine.getElector());

        Collect mySuccessful = new Collect(5, 5);

        ActCapture myActCapture = new ActCapture(mySuccessful);
        myOutdatedCapture._outdated = false;

        myMachine.dispatch(mySuccessful, myProposer, new Watermark(4, -1),
                myOutdatedCapture, myActCapture);

        Assert.assertFalse("Newer collect should supersede primordial", myOutdatedCapture._outdated);
        Assert.assertTrue(myActCapture._acted);
        Assert.assertTrue(myPreviousExpiry < myMachine.getExpiry());
        Assert.assertEquals(mySuccessful, myMachine.getElected());
        Assert.assertEquals(myProposer, myMachine.getElector());

        myMachine.resetElected();
        Assert.assertEquals(Collect.INITIAL, myMachine.getElected());
        Assert.assertNotSame(myProposer, myMachine.getElector());
    }

    @Test
    public void checkBegin() throws Exception {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());
        InetSocketAddress myProposer = TestAddresses.next();
        long myPreviousExpiry = myMachine.getExpiry();

        OutdatedCapture myOutdatedCapture = new OutdatedCapture(myProposer);

        myMachine.dispatch(new Begin(Constants.PRIMORDIAL_SEQ - 1, Constants.PRIMORDIAL_RND,
                        Proposal.NO_VALUE),
                new Watermark(Constants.PRIMORDIAL_SEQ, -1),
                myOutdatedCapture,
                (c, w) -> {});

        Assert.assertTrue("Begin at primordial sequence - 1 should be declared outdated",
                myOutdatedCapture._outdated);
        Assert.assertEquals(myPreviousExpiry, myMachine.getExpiry());

        Thread.sleep(200);

        Begin mySuccessful = new Begin(5, Constants.PRIMORDIAL_RND, Proposal.NO_VALUE);

        ActCapture myActCapture = new ActCapture(mySuccessful);
        myOutdatedCapture._outdated = false;

        myMachine.dispatch(mySuccessful, new Watermark(4, -1),
                myOutdatedCapture, myActCapture);

        Assert.assertFalse(myOutdatedCapture._outdated);
        Assert.assertTrue("Begin for later sequence and primordial rnd should progress", myActCapture._acted);
        Assert.assertTrue(myPreviousExpiry < myMachine.getExpiry());
    }

    @Test
    public void checkRecoveryBegin() {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());
        InetSocketAddress myProposer = TestAddresses.next();
        long myPreviousExpiry = myMachine.getExpiry();

        Begin myTest = new Begin(Constants.PRIMORDIAL_SEQ - 1, Constants.PRIMORDIAL_RND,
                Proposal.NO_VALUE);

        OutdatedCapture myOutdatedCapture = new OutdatedCapture(myProposer);
        ActCapture myActCapture = new ActCapture(myTest);

        myMachine.dispatch(myTest,
                new Watermark(Constants.PRIMORDIAL_SEQ, -1),
                myOutdatedCapture,
                myActCapture,
                true);

        // In recovery we accept anything because we're trusting the already validated log of another replica
        //
        Assert.assertFalse("Begin at primordial sequence - 1 should not be declared outdated",
                myOutdatedCapture._outdated);
        Assert.assertFalse(myPreviousExpiry == myMachine.getExpiry());
        Assert.assertTrue("Begin should progress", myActCapture._acted);
    }

    @Test
    public void checkRecoveryCollect() {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Consensus.StateMachine myMachine = new Consensus.StateMachine("Test", new StatsImpl());
        InetSocketAddress myProposer = TestAddresses.next();
        long myPreviousExpiry = myMachine.getExpiry();

        Collect myTest = new Collect(Constants.PRIMORDIAL_SEQ, 5);
        OutdatedCapture myOutdatedCapture = new OutdatedCapture(myProposer);
        ActCapture myActCapture = new ActCapture(myTest);

        myMachine.dispatch(myTest,
                myProposer, new Watermark(Constants.PRIMORDIAL_SEQ, -1),
                myOutdatedCapture,
                myActCapture,
                true);

        Assert.assertFalse("Primordial collect can supersede same", myOutdatedCapture._outdated);
        Assert.assertFalse(myPreviousExpiry == myMachine.getExpiry());
        Assert.assertNotSame(Collect.INITIAL, myMachine.getElected());
        Assert.assertEquals(myProposer, myMachine.getElector());
        Assert.assertTrue("Collect should progress", myActCapture._acted);
    }
}
