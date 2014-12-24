package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Need;

import java.net.InetSocketAddress;
import java.util.*;

class PacketSorter {
    private final int _maxInflight;

    private SortedMap<Long, List<Transport.Packet>> _packets = new TreeMap<>();

    PacketSorter() {
        this(Constants.DEFAULT_MAX_INFLIGHT);
    }

    PacketSorter(int aMaxInflight) {
        _maxInflight = aMaxInflight;
    }

    int numPackets() {
        int myTotal = 0;

        for (List<Transport.Packet> myPs : _packets.values())
            myTotal += myPs.size();

        return myTotal;
    }

    void add(Transport.Packet aPacket) {
        _packets = insert(aPacket, _packets);
    }

    /**
     * @param aLowWatermark the current low watermark - sorter will use this to identify packets that potentially
     *                      could be consumed.
     * @param aProcessor the processor that will be used to process any packets identified as acceptable or perform
     *                   recovery. Recovery should do an atomic test and set to see if it wins the recovery race
     *                   and act accordingly.
     * @return the number of packets processed
     */
    int process(long aLowWatermark, PacketProcessor aProcessor) {
        /*
         * Atomically remove the appropriate packets from the sorter under a lock
         *
         * Then process them outside the lock
         *
         * Packets less than the low watermark are ignored in AL.process()
         */
        List<Transport.Packet> myConsumables = new LinkedList<>();

        synchronized(this) {
            Iterator<Map.Entry<Long, List<Transport.Packet>>> mySeqsAndPkts = _packets.entrySet().iterator();

            while(mySeqsAndPkts.hasNext()) {
                Map.Entry<Long, List<Transport.Packet>> mySeqAndPkt = mySeqsAndPkts.next();

                if (mySeqAndPkt.getKey() <= (aLowWatermark + 1)) {
                    myConsumables.addAll(_packets.get(mySeqAndPkt.getKey()));
                    mySeqsAndPkts.remove();
                }
            }
        }

        if ((myConsumables.size() == 0) && (_packets.size() != 0)) {
            // Do we need to trigger recovery?
            //
            SortedSet<Long> myAllSeqs = new TreeSet<>(_packets.keySet());
            Long myLastSeq = myAllSeqs.last();

            if (myLastSeq > (aLowWatermark + _maxInflight)) {
                InetSocketAddress myTriggerAddr = _packets.get(myLastSeq).get(0).getSource();

                if (aProcessor.recover(new Need(aLowWatermark, myLastSeq - 1), myTriggerAddr)) {
                    synchronized(this) {
                        List<Transport.Packet> myLastPackets = _packets.get(myLastSeq);

                        _packets.clear();
                        _packets.put(myLastSeq, myLastPackets);
                    }
                }
            }

            return 0;
        } else {
            for (Transport.Packet p : myConsumables)
                aProcessor.consume(p);

            return myConsumables.size();
        }
    }

    private SortedMap<Long, List<Transport.Packet>> insert(Transport.Packet aPacket,
                                                     SortedMap<Long, List<Transport.Packet>> aPackets) {
        Long mySeq = aPacket.getMessage().getSeqNum();
        List<Transport.Packet> myPackets = aPackets.get(mySeq);

        if (myPackets == null) {
            myPackets = new LinkedList<>();
            _packets.put(mySeq, myPackets);
        }

        myPackets.add(aPacket);

        return aPackets;
    }

    void clear() {
        synchronized(this) {
            _packets.clear();
        }
    }

    interface PacketProcessor {
        void consume(Transport.Packet aPacket);

        /**
         * @param aNeed
         * @param aSourceAddr the node that originated the packet triggering recovery
         *
         * @return true if the transition to recovery was successful
         */
        boolean recover(Need aNeed, InetSocketAddress aSourceAddr);
    }
}
