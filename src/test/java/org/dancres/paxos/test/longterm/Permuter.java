package org.dancres.paxos.test.longterm;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.SynchronizedRandomGenerator;
import org.apache.commons.math3.random.Well44497b;

import java.util.LinkedList;
import java.util.List;

/**
 * <p>Intended use is to construct the Permuter, add <b>ALL</b> possibilities and then apply ticks.
 * If various possibilities are to be activated only at particular moments do this via additional
 * <code>Preconditions</code>.</p>
 *
 * <p>With the above in mind, <code>add</code> is <b>NOT</b> thread-safe whilst
 * <code>tick</code> <b>IS</b>.</p>
 * 
 * @param <Context> - information that might be required for operation of <code>Restoration</code>
 *                 and <code>Possibility</code> implementations.
 */
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
        synchronized (_outstandingRestorations) {
            _outstandingRestorations.removeIf(r -> r.tick(aContext));
        }

        for (Possibility<Context> myPoss : _possibilities) {
            List<Precondition<Context>> myPreconditions = myPoss.getPreconditions();

            if (myPreconditions.stream().allMatch(p -> p.isSatisfied(aContext)) &&
                    (_rng.nextInt(100) < myPoss.getChance())) {

                Restoration<Context> myRestoration = myPoss.apply(aContext, _rng);

                synchronized (_outstandingRestorations) {
                    _outstandingRestorations.add(myRestoration);
                }
            }
        }
    }

    public int numOutstanding() {
        synchronized (_outstandingRestorations) {
            return _outstandingRestorations.size();
        }
    }

    public interface Possibility<Context> {
        List<Precondition<Context>> getPreconditions();

        int getChance();

        Restoration<Context> apply(Context aContext, RandomGenerator aGen);
    }

    public void restoreOutstanding(Context aContext) {
        synchronized (this) {
            while (_outstandingRestorations.size() > 0)
                _outstandingRestorations.removeIf(r -> r.tick(aContext));
        }
    }

    public interface Restoration<Context> {
        boolean tick(Context aContext);
    }

    public interface Precondition<Context> {
        boolean isSatisfied(Context aContext);
    }
}
