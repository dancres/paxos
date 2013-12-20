package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;

import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.Map;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedMemoryNetwork implements Runnable {
    private static Logger _logger = LoggerFactory.getLogger(OrderedMemoryNetwork.class);

    public interface OrderedMemoryTransport extends Transport {
        public void distribute(Transport.Packet aPacket);
    }

    public interface Factory {
        public OrderedMemoryTransport newTransport(InetSocketAddress aLocalAddr, InetSocketAddress aBroadcastAddr,
                                                   OrderedMemoryNetwork aNetwork, MessageBasedFailureDetector anFD);
    }

    private class PacketWrapper {
        private Transport.Packet _packet;
        private InetSocketAddress _target;

        PacketWrapper(Transport.Packet aPacket, InetSocketAddress aTarget) {
            _packet = aPacket;
            _target = aTarget;
        }

        Transport.Packet getPacket() {
            return _packet;
        }

        InetSocketAddress getTarget() {
            return _target;
        }
    }

    private class DefaultFactory implements Factory {
        public OrderedMemoryTransport newTransport(InetSocketAddress aLocalAddr, InetSocketAddress aBroadcastAddr,
                                                   OrderedMemoryNetwork aNetwork, MessageBasedFailureDetector anFD) {
            return new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD);
        }
    }

    private Factory _factory = new DefaultFactory();
    private BlockingQueue<PacketWrapper> _queue = new LinkedBlockingQueue<PacketWrapper>();
    private AtomicBoolean _isStopping = new AtomicBoolean(false);
    private InetSocketAddress  _broadcastAddr;
    private Map<InetSocketAddress, OrderedMemoryTransport> _nodes =
            new ConcurrentHashMap<InetSocketAddress, OrderedMemoryTransport>();

    public OrderedMemoryNetwork() throws Exception {
        _broadcastAddr = new InetSocketAddress(org.dancres.paxos.impl.net.Utils.getBroadcastAddress(), 255);

        Thread myDispatcher = new Thread(this);

        myDispatcher.setDaemon(true);
        myDispatcher.start();
    }

    public void stop() {
        _isStopping.set(true);
    }

    void enqueue(Transport.Packet aPacket, InetSocketAddress aTarget) throws Exception {
        _queue.put(new PacketWrapper(aPacket, aTarget));
    }

    public void run() {
        while (! _isStopping.get()) {
            try {
                PacketWrapper myNext = _queue.poll(100, TimeUnit.MILLISECONDS);

                if (myNext != null) {
                    if (myNext.getTarget().equals(_broadcastAddr)) {
                        for (InetSocketAddress k : _nodes.keySet()) {
                            dispatch(new PacketWrapper(myNext.getPacket(), k));
                        }
                    } else {
                        dispatch(myNext);
                    }
                }
            } catch (Exception anE) {
                _logger.error("Failed to dispatch queue", anE);
            }
        }
    }

    private void dispatch(PacketWrapper aPayload) {
        OrderedMemoryTransport myDest = _nodes.get(aPayload.getTarget());

        if (myDest != null)
            myDest.distribute(aPayload.getPacket());
        else
            _logger.warn("Couldn't distribute packet to target: " + aPayload.getTarget());
    }

    public Transport newTransport(Factory aFactory, MessageBasedFailureDetector anFD) {
        InetSocketAddress myAddr = Utils.getTestAddress();
        OrderedMemoryTransport myTrans = (aFactory == null) ?
                _factory.newTransport(myAddr, _broadcastAddr, this, anFD) :
                aFactory.newTransport(myAddr, _broadcastAddr, this, anFD);

        _nodes.put(myAddr, myTrans);

        return myTrans;
    }

    void destroy(OrderedMemoryTransport aTransport) {
        _nodes.remove(aTransport.getLocalAddress());
    }
}