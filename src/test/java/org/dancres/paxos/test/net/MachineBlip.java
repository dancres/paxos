package org.dancres.paxos.test.net;

import org.apache.commons.math3.random.RandomGenerator;
import org.dancres.paxos.test.longterm.Environment;
import org.dancres.paxos.test.longterm.NodeAdmin;
import org.dancres.paxos.test.longterm.Permuter;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/* FOR LATER WHEN WE DO DEATH WITH NO REBIRTH
    private void considerKill() {
        for (NodeAdmin myAdmin: _env.getKillableNodes()) {
            if ((myAdmin.getRngByName("PermDeath").nextInt(101) < 1) &&
                    (_killCount.compareAndSet(0, 1))) {

                if (_env.killSpecific(myAdmin) != null)
                    _killCount.decrementAndGet();
            }
        }
    }
 */
class MachineBlip implements Permuter.Possibility<OrderedMemoryTransportImpl.Context> {
    private AtomicLong _deadCount = new AtomicLong(0);

    static class Grave implements Permuter.Restoration<OrderedMemoryTransportImpl.Context> {
        private AtomicReference<NodeAdmin.Memento> _dna = new AtomicReference<>(null);
        private AtomicLong _deadCycles = new AtomicLong(0);

        Grave(NodeAdmin.Memento aDna, long aDeadCycles) {
            _dna.set(aDna);
            _deadCycles.set(aDeadCycles);
        }

        private void awaken(Environment anEnv) {
            OrderedMemoryTransportImpl._logger.info("Awaking from grave: " + _dna.get());

            NodeAdmin.Memento myDna = _dna.getAndSet(null);

            if (myDna != null)
                anEnv.addNodeAdmin(myDna);
        }

        public boolean tick(OrderedMemoryTransportImpl.Context aContext) {
            if (_deadCycles.decrementAndGet() == 0) {
                awaken(aContext._transport.getEnv());

                return true;
            }

            return false;
        }
    }

    @Override
    public List<Permuter.Precondition<OrderedMemoryTransportImpl.Context>> getPreconditions() {
        return List.of(c -> _deadCount.get() == 0,
                c -> !c._transport.getEnv().isSettling(),
                c -> c._transport.getEnv().isReady());
    }

    @Override
    public int getChance() {
        return 1;
    }

    @Override
    public Permuter.Restoration<OrderedMemoryTransportImpl.Context> apply(OrderedMemoryTransportImpl.Context aContext, RandomGenerator aGen) {
        Environment myEnv = aContext._transport.getEnv();

        for (NodeAdmin myAdmin: myEnv.getKillableNodes()) {
            if ((! myAdmin.getTransport().getLocalAddress().equals(aContext._packet.getSource())) &&
                    (_deadCount.compareAndSet(0, 1))) {

                int myRebirthTicks;

                while ((myRebirthTicks = aGen.nextInt(500)) == 0);

                NodeAdmin.Memento myMemento = myEnv.killSpecific(myAdmin);

                if (myMemento != null) {
                    OrderedMemoryTransportImpl._logger.info("Grave dug for " + myMemento + " with return after " + myRebirthTicks +
                            " and we're " + (_deadCount.get()) + " nodes down");
                    Grave myGrave = new Grave(myMemento, myRebirthTicks);
                    
                    return myGrave;
                } else {
                    _deadCount.decrementAndGet();
                }
            }
        }

        return (c) -> true;
    }
}

