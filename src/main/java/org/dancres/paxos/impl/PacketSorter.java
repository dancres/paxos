package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Need;

import java.util.*;

public class PacketSorter {
    private static final long MAX_INFLIGHT = 1;

    private SortedMap<Long, List<Transport.Packet>> _packets = new TreeMap<Long, List<Transport.Packet>>();

    public int numPackets() {
        int myTotal = 0;

        for (Long myS : _packets.keySet())
            myTotal += _packets.get(myS).size();

        return myTotal;
    }

    /**
     * @param aPacket the packet we're adding to the sorter
     * @param aLowWatermark the current low watermark - sorter will use this to identify packets that potentially
     *                      could be consumed.
     * @param aProcessor the processor that will be used to process any packets identified as acceptable or perform
     *                   recovery. Recovery should do an atomic test and set to see if it wins the recovery race
     *                   and act accordingly.
     */
    public void process(Transport.Packet aPacket, long aLowWatermark, PacketProcessor aProcessor) {
        /*
         * Atomically remove the appropriate packets from the sorter under a lock
         *
         * Then process them outside the lock
         *
         * Packets less than the low watermark are ignored in AL.process()
         */
        List<Transport.Packet> myConsumables = new LinkedList<Transport.Packet>();

        synchronized(this) {
            _packets = insert(aPacket, _packets);

            for (Long myS : _packets.keySet()) {
                if (myS <= (aLowWatermark + 1)) {
                    myConsumables.addAll(_packets.remove(myS));
                }
            }
        }

        if (myConsumables.size() == 0) {
            // Do we need to trigger recovery?
            //
            SortedSet<Long> myAllSeqs = new TreeSet<Long>(_packets.keySet());

            if (myAllSeqs.last() > (aLowWatermark + MAX_INFLIGHT))
                if (aProcessor.recover(new Need(aLowWatermark, myAllSeqs.last() - 1))) {
                    _packets.clear();
                    insert(aPacket, _packets);
                }
        } else {
            for (Transport.Packet p : myConsumables)
                aProcessor.consume(p);
        }
    }

    private SortedMap<Long, List<Transport.Packet>> insert(Transport.Packet aPacket,
                                                     SortedMap<Long, List<Transport.Packet>> aPackets) {
        Long mySeq = aPacket.getMessage().getSeqNum();
        List<Transport.Packet> myPackets = aPackets.get(mySeq);

        if (myPackets == null) {
            myPackets = new LinkedList<Transport.Packet>();
            _packets.put(mySeq, myPackets);
        }

        myPackets.add(aPacket);

        return aPackets;
    }

    public interface PacketProcessor {
        void consume(Transport.Packet aPacket);

        /**
         * @param aNeed
         * @return true if the transition to recovery was successful
         */
        boolean recover(Need aNeed);
    }
}
