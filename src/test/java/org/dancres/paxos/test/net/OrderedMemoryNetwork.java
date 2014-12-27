package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;

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
        public class Constructed {
            private final OrderedMemoryTransport _tp;
            private final Object _add;

            public Constructed(OrderedMemoryTransport aTransport, Object anAdditional) {
                _tp = aTransport;
                _add = anAdditional;
            }

            public OrderedMemoryTransport getTransport() {
                return _tp;
            }

            public Object getAdditional() {
                return _add;
            }
        }

        public Constructed newTransport(InetSocketAddress aLocalAddr, InetSocketAddress aBroadcastAddr,
                                                   OrderedMemoryNetwork aNetwork, MessageBasedFailureDetector anFD,
                                                   Object aContext);
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
        public Constructed newTransport(InetSocketAddress aLocalAddr, InetSocketAddress aBroadcastAddr,
                                                   OrderedMemoryNetwork aNetwork, MessageBasedFailureDetector anFD,
                                                   Object aContext) {
            return new Constructed(new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD), null);
        }
    }

    private Factory _factory = new DefaultFactory();
    private BlockingQueue<PacketWrapper> _queue = new LinkedBlockingQueue<>();
    private AtomicBoolean _isStopping = new AtomicBoolean(false);
    private InetSocketAddress  _broadcastAddr;
    private Map<InetSocketAddress, OrderedMemoryTransport> _transports =
            new ConcurrentHashMap<>();

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
                        for (InetSocketAddress k : _transports.keySet()) {
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
        OrderedMemoryTransport myDest = _transports.get(aPayload.getTarget());

        if (myDest != null)
            myDest.distribute(aPayload.getPacket());
        else
            _logger.warn("Couldn't distribute packet to target: " + aPayload.getTarget());
    }

    public Factory.Constructed newTransport(Factory aFactory, MessageBasedFailureDetector anFD, InetSocketAddress anAddr,
                                  Object aContext) {
        Factory.Constructed myResult = (aFactory == null) ?
                _factory.newTransport(anAddr, _broadcastAddr, this, anFD, aContext) :
                aFactory.newTransport(anAddr, _broadcastAddr, this, anFD, aContext);

        _transports.put(anAddr, myResult.getTransport());

        return myResult;
    }

    void destroy(OrderedMemoryTransport aTransport) {
        _transports.remove(aTransport.getLocalAddress());
    }
}