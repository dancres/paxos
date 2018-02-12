package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Claim;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.test.net.Utils;
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

        Assert.assertTrue(Protocol.isOldSeq.apply(myBefore, myPointInTime));
        Assert.assertFalse(Protocol.isOldSeq.apply(myAfter, myPointInTime));
    }

    @Test
    public void validateRndChecks() {
        Collect myPast = new Collect(Constants.PRIMORDIAL_SEQ, 4);
        Collect myElected = new Collect(Constants.PRIMORDIAL_SEQ, 6);

        Assert.assertTrue(Protocol.isPastRnd.apply(myPast, myElected));
        Assert.assertFalse(Protocol.isPastRnd.apply(myElected, myPast));

        Collect myElectable = new Collect(Constants.PRIMORDIAL_SEQ, 8);

        Assert.assertTrue(Protocol.isElectableRnd.apply(myElectable, myElected));
        Assert.assertFalse(Protocol.isElectableRnd.apply(myPast, myElected));

        Begin mySameRnd = new Begin(Constants.PRIMORDIAL_SEQ, myElected.getRndNumber(), null);
        Assert.assertTrue(Protocol.isTheElectedRnd.apply(mySameRnd, myElected));

        Assert.assertTrue(Protocol.shouldWriteCollect.apply(myElectable, myElected));
        Assert.assertFalse(Protocol.shouldWriteCollect.apply(myPast, myElected));
    }

    @Test
    public void validateCollectOld() {
        Protocol.StateMachine myMachine = new Protocol.StateMachine();

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
        Protocol.StateMachine myMachine = new Protocol.StateMachine();
        InetSocketAddress myAddrOne = Utils.getTestAddress();
        InetSocketAddress myAddrTwo = Utils.getTestAddress();

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
        Protocol.StateMachine myMachine = new Protocol.StateMachine();

        Assert.assertTrue("Begin is for old round but newer sequence number",
                myMachine.old(new Begin(2, 5, null),
                new Collect(2, 6), new Watermark(1, -1)));

        Assert.assertTrue("Begin is for current round but older sequence number",
                myMachine.old(new Begin(1, 6, null),
                        new Collect(2, 6), new Watermark(2, -1)));

        Assert.assertFalse("Begin is for current round and newer sequence number",
                myMachine.old(new Begin(3, 6, null),
                        new Collect(2, 6), new Watermark(2, -1)));
    }

    @Test
    public void validateBeginActionable() {
        Protocol.StateMachine myMachine = new Protocol.StateMachine();

        Assert.assertFalse("Not from elected, too low",
                myMachine.actionable(new Begin(5, 4, null),
                new Collect(5, 5)));

        Assert.assertFalse("Not from elected, too high",
                myMachine.actionable(new Begin(5, 10, null),
                        new Collect(5, 5)));

        Assert.assertTrue("From elected",
                myMachine.actionable(new Begin(5, 10, null),
                        new Collect(5, 10)));
    }

    static class ActCapture implements Protocol.Act {
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

    @Test
    public void checkCollect() {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Protocol.StateMachine myMachine = new Protocol.StateMachine();
        InetSocketAddress myProposer = Utils.getTestAddress();
        long myPreviousExpiry = myMachine.getExpiry();

        class OutdatedCapture implements Protocol.Outdated {
            boolean _outdated = false;

            @Override
            public void accept(Long aLong, InetSocketAddress anAddress) {
                Assert.assertTrue(Collect.INITIAL.getRndNumber() == aLong);
                Assert.assertNotSame(myProposer, anAddress);
                _outdated = true;
            }
        };

        OutdatedCapture myOutdatedCapture = new OutdatedCapture();

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
    }

    @Test
    public void checkBegin() throws Exception {
        // StateMachine will be initialised with Collect.INITIAL, hence PRIMORDIAL rnd and sequence number
        //
        Protocol.StateMachine myMachine = new Protocol.StateMachine();
        InetSocketAddress myProposer = Utils.getTestAddress();
        long myPreviousExpiry = myMachine.getExpiry();

        class OutdatedCapture implements Protocol.Outdated {
            boolean _outdated = false;

            @Override
            public void accept(Long aLong, InetSocketAddress anAddress) {
                Assert.assertTrue(Collect.INITIAL.getRndNumber() == aLong);
                Assert.assertNotSame(myProposer, anAddress);
                _outdated = true;
            }
        };

        OutdatedCapture myOutdatedCapture = new OutdatedCapture();

        myMachine.dispatch(new Begin(Constants.PRIMORDIAL_SEQ - 1, Constants.PRIMORDIAL_RND, null),
                new Watermark(Constants.PRIMORDIAL_SEQ, -1),
                myOutdatedCapture,
                (c, w) -> {});

        Assert.assertTrue("Begin at primordial sequence - 1 should be declared outdated",
                myOutdatedCapture._outdated);
        Assert.assertEquals(myPreviousExpiry, myMachine.getExpiry());

        Thread.sleep(200);

        Begin mySuccessful = new Begin(5, Constants.PRIMORDIAL_RND, null);

        ActCapture myActCapture = new ActCapture(mySuccessful);
        myOutdatedCapture._outdated = false;

        myMachine.dispatch(mySuccessful, new Watermark(4, -1),
                myOutdatedCapture, myActCapture);

        Assert.assertFalse(myOutdatedCapture._outdated);
        Assert.assertTrue("Begin for later sequence and primordial rnd should progress", myActCapture._acted);
        Assert.assertTrue(myPreviousExpiry < myMachine.getExpiry());
    }
}
