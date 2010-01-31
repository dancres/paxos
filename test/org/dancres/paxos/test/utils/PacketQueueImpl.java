package org.dancres.paxos.test.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacketQueueImpl implements PacketQueue, Runnable {
    private static Logger _logger = LoggerFactory.getLogger(PacketQueueImpl.class);

    private BlockingQueue<PaxosMessage> _queue = new ArrayBlockingQueue<PaxosMessage>(10);
    private PacketListener _listener;

    /**
     * For unit testing only
     */
    public PacketQueueImpl() {
    }

    public PacketQueueImpl(PacketListener aListener) {
        Thread myDaemon = new Thread(this);
        myDaemon.setDaemon(true);
        myDaemon.start();
        _listener = aListener;
    }

    public void add(PaxosMessage aMessage) {
        _queue.add(aMessage);
    }

    public void run() {
        while (true) {
            try {
                PaxosMessage myMessage = _queue.take();
                _listener.deliver(myMessage);
            } catch (Exception anE) {
                _logger.error("Couldn't get message", anE);
            }
        }
    }

    public PaxosMessage getNext(long aPause) throws InterruptedException {
        return _queue.poll(aPause, TimeUnit.MILLISECONDS);
    }
}
