package org.dancres.paxos.test.longterm;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PermuterTest {
    class NeverTry implements Permuter.Possibility<Object> {
        private List<Permuter.Precondition<Object>> _preconditions = List.of(o -> false);

        @Override
        public List<Permuter.Precondition<Object>> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            throw new IllegalStateException("Should never happen");
        }

        @Override
        public Permuter.Restoration<Object> apply(Object aContext, RandomGenerator anRNG) {
            throw new IllegalStateException("Should never happen");
        }
    }

    class RestoreAll implements Permuter.Possibility<Object> {
        private boolean _wasRestored = false;
        private List<Permuter.Precondition<Object>> _preconditions = List.of(o -> true);

        @Override
        public List<Permuter.Precondition<Object>> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            return 100;
        }

        @Override
        public Permuter.Restoration<Object> apply(Object aContext, RandomGenerator anRNG) {
            return (c) -> {
                _wasRestored = true;
                return true;
            };
        }

        boolean wasRestored() {
            return _wasRestored;
        }

    }
    class ZeroChance implements Permuter.Possibility<Object> {
        private List<Permuter.Precondition<Object>> _preconditions = List.of(o -> true);

        @Override
        public List<Permuter.Precondition<Object>> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            return 0;
        }

        @Override
        public Permuter.Restoration<Object> apply(Object aContext, RandomGenerator aGen) {
            throw new IllegalStateException("Should never happen");
        }
    }

    class OneShot implements Permuter.Possibility<Object> {
        private boolean _actionFired = false;
        private List<Permuter.Precondition<Object>> _preconditions = List.of(o -> !_actionFired);

        @Override
        public List<Permuter.Precondition<Object>> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            return 100;
        }

        @Override
        public Permuter.Restoration<Object> apply(Object aContext, RandomGenerator anRNG) {
            _actionFired = true;

            return new Permuter.Restoration<>() {
                private int _ticks = 0;

                @Override
                public boolean tick(Object aContext) {
                    ++_ticks;

                    return _ticks == 100;
                }
            };
        }

        boolean wasFired() {
            return _actionFired;
        }
    }

    @Test
    public void neverTry() {
        Permuter<Object> myPermuter = new Permuter<>();
        NeverTry myPoss = new NeverTry();

        myPermuter.add(myPoss);

        for (int myCycle = 0; myCycle < 100; myCycle++)
            myPermuter.tick(new Object());
    }

    @Test
    public void neverShot() {
        Permuter<Object> myPermuter = new Permuter<>();
        ZeroChance myPoss = new ZeroChance();

        myPermuter.add(myPoss);

        for (int myCycle = 0; myCycle < 100; myCycle++)
            myPermuter.tick(new Object());
    }

    @Test
    public void firesOnlyOnTick() {
        Permuter<Object> myPermuter = new Permuter<>();
        OneShot myPoss = new OneShot();

        myPermuter.add(myPoss);

        Assert.assertFalse(myPoss.wasFired());

        myPermuter.tick(new Object());

        Assert.assertTrue(myPoss.wasFired());
    }

    @Test
    public void oneShot() {
        Permuter<Object> myPermuter = new Permuter<>();
        OneShot myPoss = new OneShot();

        myPermuter.add(myPoss);
        myPermuter.tick(new Object());

        // At the 100th tick the restoration should occur
        //
        for (int myCycle = 0; myCycle < 100; myCycle++) {
            Assert.assertTrue(myPermuter.numOutstanding() == 1);
            myPermuter.tick(new Object());
        }

        // Effect of the last tick in the loop is checked here
        //
        Assert.assertTrue(myPermuter.numOutstanding() == 0);
    }

    @Test
    public void restoreAll() {
        Permuter<Object> myPermuter = new Permuter<>();
        RestoreAll myR1 = new RestoreAll();
        RestoreAll myR2 = new RestoreAll();


        myPermuter.add(myR1).add(myR2);
        myPermuter.tick(new Object());

        myPermuter.restoreOutstanding(new Object());

        Assert.assertTrue(myR1.wasRestored());
        Assert.assertTrue(myR2.wasRestored());
        
        Assert.assertTrue(myPermuter.numOutstanding() == 0);
    }
}
