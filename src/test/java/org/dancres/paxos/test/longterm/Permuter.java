package org.dancres.paxos.test.longterm;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.LinkedList;
import java.util.List;

public class Permuter<Context> {
    private final RandomGenerator _rng;
    private final List<Restoration<Context>> _outstandingRestorations = new LinkedList<>();
    private final List<Possibility<Context>> _possibilities = new LinkedList<>();

    public Permuter(long aSeed) {
         _rng = new SynchronizedRandomGenerator(new Well44497b(aSeed));
    }

    public Permuter() {
        _rng = new SynchronizedRandomGenerator(new Well44497b());
    }

    public Permuter<Context> add(Possibility<Context> aPoss) {
        _possibilities.add(aPoss);
        return this;
    }

    public void tick(Context aContext) {
        _outstandingRestorations.removeIf(r -> r.tick(aContext));

        for (Possibility<Context> myPoss : _possibilities) {
            List<Precondition<Context>> myPreconditions = myPoss.getPreconditions();

            if (myPreconditions.stream().allMatch(p -> p.isSatisfied(aContext)) &&
                    (_rng.nextInt(100) < myPoss.getChance())) {

                _outstandingRestorations.add(myPoss.apply(aContext, _rng));
            }
        }
    }

    public int numOutstanding() {
        return _outstandingRestorations.size();
    }

    public interface Possibility<Context> {
        List<Precondition<Context>> getPreconditions();

        int getChance();

        Restoration<Context> apply(Context aContext, RandomGenerator aGen);
    }

    public void restoreOutstanding(Context aContext) {
        while(_outstandingRestorations.size() > 0)
            _outstandingRestorations.removeIf(r -> r.tick(aContext));
    }

    public interface Restoration<Context> {
        boolean tick(Context aContext);
    }

    public interface Precondition<Context> {
        boolean isSatisfied(Context aContext);
    }
}
