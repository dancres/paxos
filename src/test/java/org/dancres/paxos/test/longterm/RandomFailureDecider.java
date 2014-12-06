package org.dancres.paxos.test.longterm;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: This should schedule the recovery of a dead machine based on a certain
 * number of iterations of the protocol so we can fence it to within the bounds
 * of a single snapshot or let it cross, depending on the sophistication/challenge
 * of testing we require.
 */
class RandomFailureDecider implements Decider {
    private static final Logger _logger = LoggerFactory.getLogger(RandomFailureDecider.class);

    static class Grave {
        private AtomicReference<NodeAdmin.Memento> _dna = new AtomicReference<>(null);
        private AtomicLong _deadCycles = new AtomicLong(0);

        Grave(NodeAdmin.Memento aDna, long aDeadCycles) {
            _dna.set(aDna);
            _deadCycles.set(aDeadCycles);
        }

        void awaken(Environment anEnv) {
            _logger.info("Awaking from grave: " + _dna.get());

            NodeAdmin.Memento myDna = _dna.getAndSet(null);

            if (myDna != null)
                anEnv.addNodeAdmin(myDna);
        }

        boolean reborn(Environment anEnv) {
            if (_deadCycles.decrementAndGet() == 0) {
                awaken(anEnv);

                return true;
            }

            return false;
        }
    }

    private final Environment _env;
    private final Random _rng;

    private final AtomicLong _killCount = new AtomicLong(0);
    private final AtomicLong _deadCount = new AtomicLong(0);
    private final Deque<Grave> _graves = new ConcurrentLinkedDeque<>();

    private final AtomicLong _dropCount = new AtomicLong(0);
    private final AtomicLong _packetsTx = new AtomicLong(0);
    private final AtomicLong _packetsRx = new AtomicLong(0);

    RandomFailureDecider(Environment anEnv) {
        _env = anEnv;
        _rng = new Random(_env.getRng().nextLong());

    }

    public boolean sendUnreliable(Transport.Packet aPacket) {
        _packetsTx.incrementAndGet();

        return decide(aPacket);
    }

    public boolean receive(Transport.Packet aPacket) {
        _packetsRx.incrementAndGet();

        return decide(aPacket);
    }

    private boolean decide(Transport.Packet aPacket) {

        // Client isn't written to cope with failure handling
        //
        if (aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT))
            return true;

        Iterator<Grave> myGraves = _graves.iterator();

        while (myGraves.hasNext()) {
            Grave myGrave = myGraves.next();

            if (myGrave.reborn(_env)) {
                _deadCount.decrementAndGet();

                _logger.info("We're now " + _deadCount.get() + " nodes down");

                _graves.remove();
            }
        }

        if (! _env.isSettling()) {
            if (_rng.nextInt(101) < 2) {
                _dropCount.incrementAndGet();
                return false;
            } else {
                // considerKill();
                considerTempDeath();
            }
        }

        return true;
    }

    private void considerKill() {
        for (NodeAdmin myAdmin: _env.getNodes()) {
            if ((myAdmin.getRngByName("PermDeath").nextInt(101) < 1) &&
                    (_killCount.compareAndSet(0, 1))) {

                if (_env.killSpecific(myAdmin) != null)
                    _killCount.decrementAndGet();
            }
        }
    }

    /**
     * @todo Update to allow temp death of more than one node in parallel.
     */
    private void considerTempDeath() {
        for (NodeAdmin myAdmin: _env.getNodes()) {
            if ((myAdmin.getRngByName("TmpDeath").nextInt(101) < 1) &&
                (_deadCount.compareAndSet(0, 1))) {

                int myRebirthPackets;

                while ((myRebirthPackets = myAdmin.getRngByName("Rebirth").nextInt(500)) == 0);

                NodeAdmin.Memento myMemento = _env.killSpecific(myAdmin);

                if (myMemento != null) {
                    _logger.info("Grave dug for " + myMemento + " with return @ " + myRebirthPackets +
                            " and we're " + (_deadCount.get()) + " nodes down");

                    _graves.add(new Grave(myMemento, myRebirthPackets));
                } else {
                    _deadCount.decrementAndGet();
                }
            }
        }
    }

    public void settle() {
        Iterator<Grave> myGraves = _graves.iterator();

        while (myGraves.hasNext()) {
            Grave myGrave = myGraves.next();

            myGrave.awaken(_env);
            _graves.remove();
        }

        _killCount.set(0);
        _deadCount.set(0);
    }

    public long getDropCount() {
        return _dropCount.get();
    }

    public long getRxPacketCount() {
        return _packetsRx.get();
    }

    public long getTxPacketCount() {
        return _packetsTx.get();
    }
}

