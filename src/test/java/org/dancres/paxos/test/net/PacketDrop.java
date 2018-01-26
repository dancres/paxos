package org.dancres.paxos.test.net;

import org.apache.commons.math3.random.RandomGenerator;
import org.dancres.paxos.test.longterm.Permuter;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class PacketDrop implements Permuter.Possibility<OrderedMemoryNetwork.Context> {
    class Interruption implements Permuter.Restoration<OrderedMemoryNetwork.Context> {
        private final AtomicLong _deadCycles;
        private final OrderedMemoryTransportImpl _transport;
        private final Consumer<OrderedMemoryTransportImpl> _restorer;

        public Interruption(OrderedMemoryTransportImpl aTransport,
                            RandomGenerator aGen) {
            _transport = aTransport;
            _deadCycles = new AtomicLong(aGen.nextInt(16) + 20);

            List<Consumer<OrderedMemoryTransportImpl>> myDroppers = _transport.getDroppers();
            int myIndex = aGen.nextInt(myDroppers.size());
            
            myDroppers.get(myIndex).accept(_transport);
            _restorer = _transport.getRestorers().get(myIndex);
        }

        private void awaken(OrderedMemoryNetwork.Context aContext) {
            _restorer.accept(_transport);
            _currentInterruptions.remove(_transport.getLocalAddress());
        }

        @Override
        public boolean tick(OrderedMemoryNetwork.Context aContext) {
            if (_deadCycles.decrementAndGet() == 0) {
                awaken(aContext);
                return true;
            }

            return false;
        }
    }

    private ConcurrentMap<InetSocketAddress, Interruption> _currentInterruptions = new ConcurrentHashMap<>();

    @Override
    public List<Permuter.Precondition<OrderedMemoryNetwork.Context>> getPreconditions() {
        return List.of(c -> !c._transport.getEnv().isSettling(),
                c -> c._transport.getEnv().isReady(),
                c -> _currentInterruptions.size() < 2);
    }

    @Override
    public double getChance() {
        return 0.25;
    }

    @Override
    public Permuter.Restoration<OrderedMemoryNetwork.Context> apply(OrderedMemoryNetwork.Context aContext,
                                                                    RandomGenerator aGen) {
        Map<InetSocketAddress, OrderedMemoryTransportImpl>
                myTransports = aContext._transport.getEnv().getFactory().getTransports();

        for (InetSocketAddress myA : _currentInterruptions.keySet())
            myTransports.remove(myA);

        if (myTransports.size() == 0)
            return (c) -> true;

        InetSocketAddress[] myCandidates = new InetSocketAddress[myTransports.size()];
        myTransports.keySet().toArray(myCandidates);

        InetSocketAddress myChoice = myCandidates[aGen.nextInt(myCandidates.length)];

        // Best endeavours to ensure we honour the total active packet losses constraint
        //
        if (_currentInterruptions.size() >= 2)
            return (c) -> true;

        Interruption myInterruption = new Interruption(myTransports.get(myChoice), aGen);
        _currentInterruptions.put(myChoice, myInterruption);

        return myInterruption;
    }
}

