package org.dancres.paxos.test.longterm;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.LinkedList;
import java.util.List;

class Permuter {
    private final RandomGenerator _rng = new Well44497b();
    private final List<Restoration> _outstandingRestorations = new LinkedList<>();
    private final List<Possibility> _possibilities = new LinkedList<>();

    void add(Possibility aPoss) {
        _possibilities.add(aPoss);
    }

    void tick() {
        _outstandingRestorations.removeIf(Restoration::tick);

        for (Possibility myPoss : _possibilities) {
            List<Precondition> myPreconditions = myPoss.getPreconditions();

            if (myPreconditions.stream().allMatch(Precondition::isSatisfied) &&
                    (_rng.nextInt(100) < myPoss.getChance())) {

                _outstandingRestorations.add(myPoss.apply());
            }
        }
    }

    public int numOutstanding() {
        return _outstandingRestorations.size();
    }

    interface Possibility {
        List<Precondition> getPreconditions();

        int getChance();

        Restoration apply();
    }

    interface Restoration {
        boolean tick();
    }

    interface Precondition {
        boolean isSatisfied();
    }
}
