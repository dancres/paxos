package org.dancres.paxos.test.longterm;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.LinkedList;
import java.util.List;

public class Permuter<Context> {
    private final RandomGenerator _rng;
    private final List<Restoration> _outstandingRestorations = new LinkedList<>();
    private final List<Possibility<Context>> _possibilities = new LinkedList<>();
    
    public Permuter(long aSeed) {
         _rng = new SynchronizedRandomGenerator(new Well44497b(aSeed));
    }

    public Permuter() {
        _rng = new SynchronizedRandomGenerator(new Well44497b());
    }
    
    public void add(Possibility<Context> aPoss) {
        _possibilities.add(aPoss);
    }

    public void tick(Context aContext) {
        _outstandingRestorations.removeIf(Restoration::tick);

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

        Restoration apply(Context aContext, RandomGenerator aGen);
    }

    public interface Restoration {
        boolean tick();
    }

    public interface Precondition<Context> {
        boolean isSatisfied(Context aContext);
    }
}
