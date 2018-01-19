package org.dancres.paxos.test.longterm;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class PermuterTest {
    class NeverTry implements Permuter.Possibility {
        private List<Permuter.Precondition> _preconditions = List.of(() -> false);

        @Override
        public List<Permuter.Precondition> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            throw new IllegalStateException("Should never happen");
        }

        @Override
        public Permuter.Restoration apply() {
            throw new IllegalStateException("Should never happen");
        }
    }

    class ZeroChance implements Permuter.Possibility {
        private List<Permuter.Precondition> _preconditions = List.of(() -> true);

        @Override
        public List<Permuter.Precondition> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            return 0;
        }

        @Override
        public Permuter.Restoration apply() {
            throw new IllegalStateException("Should never happen");
        }
    }

    class OneShot implements Permuter.Possibility {
        private boolean _actionFired = false;
        private List<Permuter.Precondition> _preconditions = List.of(() -> !_actionFired);

        @Override
        public List<Permuter.Precondition> getPreconditions() {
            return _preconditions;
        }

        @Override
        public int getChance() {
            return 100;
        }

        @Override
        public Permuter.Restoration apply() {
            _actionFired = true;

            return new Permuter.Restoration() {
                private int _ticks = 0;

                @Override
                public boolean tick() {
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
        Permuter myPermuter = new Permuter();
        NeverTry myPoss = new NeverTry();

        myPermuter.add(myPoss);

        for (int myCycle = 0; myCycle < 100; myCycle++)
            myPermuter.tick();
    }

    @Test
    public void neverShot() {
        Permuter myPermuter = new Permuter();
        ZeroChance myPoss = new ZeroChance();

        myPermuter.add(myPoss);

        for (int myCycle = 0; myCycle < 100; myCycle++)
            myPermuter.tick();
    }

    @Test
    public void oneShot() {
        Permuter myPermuter = new Permuter();
        OneShot myPoss = new OneShot();

        myPermuter.add(myPoss);

        myPermuter.tick();

        Assert.assertTrue(myPoss.wasFired());

        for (int myCycle = 0; myCycle < 100; myCycle++) {
            myPermuter.tick();
            System.err.println("Permuter outstanding: " + myPermuter.numOutstanding());

            // Assert.assertTrue(myPermuter.numOutstanding() == 1);
        }

        System.err.println("Permuter: " + myPermuter);
        System.err.println("Permuter outstanding: " + myPermuter.numOutstanding());

        Assert.assertTrue(myPermuter.numOutstanding() == 0);
    }
}
