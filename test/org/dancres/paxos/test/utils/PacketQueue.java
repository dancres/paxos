package org.dancres.paxos.test.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacketQueue implements Runnable {
    private static Logger _logger = LoggerFactory.getLogger(PacketQueue.class);

    private BlockingQueue<Packet> _queue = new ArrayBlockingQueue(10);
    private PacketListener _listener;

    /**
     * For unit testing only
     */
    public PacketQueue() {
    }

    public PacketQueue(PacketListener aListener) {
        Thread myDaemon = new Thread(this);
        myDaemon.setDaemon(true);
        myDaemon.start();
        _listener = aListener;
    }

    public void add(Packet aPacket) {
        _queue.add(aPacket);
    }

    public void run() {
        while (true) {
            try {
                Packet myPacket = _queue.take();
                _listener.deliver(myPacket);
            } catch (Exception anE) {
                _logger.error("Couldn't get packet", anE);
            }
        }
    }

    public Packet getNext(long aPause) throws InterruptedException {
        return _queue.poll(aPause, TimeUnit.MILLISECONDS);
    }
}
