package org.dancres.paxos.test.net;

import org.apache.commons.math3.random.RandomGenerator;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.longterm.Permuter;

import java.util.List;

public class PacketDrop implements Permuter.Possibility<OrderedMemoryNetwork.Context> {
    @Override
    public List<Permuter.Precondition<OrderedMemoryNetwork.Context>> getPreconditions() {
        return List.of(c ->
                        !c._packet.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT),
                c -> !c._transport.getEnv().isSettling(),
                c -> c._transport.getEnv().isReady());
    }

    @Override
    public double getChance() {
        return 1.0;
    }

    @Override
    public Permuter.Restoration<OrderedMemoryNetwork.Context> apply(OrderedMemoryNetwork.Context aContext, RandomGenerator aGen) {
        aContext._transport.setDrop();

        return (c) -> true;
    }
}

