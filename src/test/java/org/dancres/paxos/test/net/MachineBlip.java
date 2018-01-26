package org.dancres.paxos.test.net;

import org.apache.commons.math3.random.RandomGenerator;
import org.dancres.paxos.test.longterm.Environment;
import org.dancres.paxos.test.longterm.NodeAdmin;
import org.dancres.paxos.test.longterm.Permuter;

import java.util.List;
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
public class MachineBlip implements Permuter.Possibility<OrderedMemoryNetwork.Context> {
    private final AtomicLong _deadCount = new AtomicLong(0);
    private final AtomicLong _graveIdGen = new AtomicLong(0);

    class Grave implements Permuter.Restoration<OrderedMemoryNetwork.Context> {
        private final AtomicReference<NodeAdmin.Memento> _dna = new AtomicReference<>(null);
        private final AtomicLong _deadCycles = new AtomicLong(0);
        private final long _id = _graveIdGen.incrementAndGet();

        Grave(NodeAdmin.Memento aDna, long aDeadCycles) {
            _dna.set(aDna);
            _deadCycles.set(aDeadCycles);
        }

        private void awaken(Environment anEnv) {
            OrderedMemoryTransportImpl._logger.info("!!!!!!!! AWAKING FROM GRAVE ID: " +
                    _id + ", " + _dna.get() + " !!!!!!!!");

            NodeAdmin.Memento myDna = _dna.getAndSet(null);

            if (myDna != null)
                anEnv.addNodeAdmin(myDna);

            _deadCount.decrementAndGet();
        }

        public boolean tick(OrderedMemoryNetwork.Context aContext) {
            if (_deadCycles.decrementAndGet() == 0) {
                awaken(aContext._transport.getEnv());

                return true;
            }

            return false;
        }

        long getId() {
            return _id;
        }
    }

    @Override
    public List<Permuter.Precondition<OrderedMemoryNetwork.Context>> getPreconditions() {
        return List.of(c -> _deadCount.get() == 0,
                c -> !c._transport.getEnv().isSettling(),
                c -> c._transport.getEnv().isReady());
    }

    @Override
    public double getChance() {
        return 0.5;
    }

    @Override
    public Permuter.Restoration<OrderedMemoryNetwork.Context> apply(OrderedMemoryNetwork.Context aContext, RandomGenerator aGen) {
        Environment myEnv = aContext._transport.getEnv();

        for (NodeAdmin myAdmin: myEnv.getKillableNodes()) {
            if ((! myAdmin.getTransport().getLocalAddress().equals(aContext._packet.getSource())) &&
                    (_deadCount.compareAndSet(0, 1))) {

                int myRebirthTicks = aGen.nextInt(501) + 500;
                NodeAdmin.Memento myMemento = myEnv.killSpecific(myAdmin);

                if (myMemento != null) {
                    Grave myGrave = new Grave(myMemento, myRebirthTicks);
                    OrderedMemoryTransportImpl._logger.info("!!!!!!!! GRAVE ID: " + myGrave.getId() + " DUG FOR " + myMemento + " with return after " + myRebirthTicks +
                            " (" + (_deadCount.get()) + " nodes down) !!!!!!!!");

                    return myGrave;
                } else {
                    _deadCount.decrementAndGet();
                }
            }
        }

        return (c) -> true;
    }
}

