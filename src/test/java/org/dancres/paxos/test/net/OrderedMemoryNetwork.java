package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;

import org.dancres.paxos.test.longterm.Environment;
import org.dancres.paxos.test.longterm.Permuter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedMemoryNetwork implements Runnable {
    private static Logger _logger = LoggerFactory.getLogger(OrderedMemoryNetwork.class);

    public interface OrderedMemoryTransport extends Transport {
        void distribute(Transport.Packet aPacket);
    }

    public static class Context {
        final Transport.Packet _packet;
        final OrderedMemoryTransportImpl _transport;

        Context(Transport.Packet aPacket, OrderedMemoryTransportImpl aTransport) {
            _packet = aPacket;
            _transport = aTransport;
        }
    }

    public interface Factory {
        class Constructed {
            private final OrderedMemoryTransportImpl _tp;
            private final Object _add;

            public Constructed(OrderedMemoryTransportImpl aTransport, Object anAdditional) {
                _tp = aTransport;
                _add = anAdditional;
            }

            public OrderedMemoryTransportImpl getTransport() {
                return _tp;
            }

            public Object getAdditional() {
                return _add;
            }
        }

        Constructed newTransport(InetSocketAddress aLocalAddr, InetSocketAddress aBroadcastAddr,
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
    private Map<InetSocketAddress, OrderedMemoryTransportImpl> _transports =
            new ConcurrentHashMap<>();
    private final Environment _env;
    private final Permuter<Context> _permuter;

    public OrderedMemoryNetwork(Environment anEnv) {
        _env = anEnv;
        _broadcastAddr = new InetSocketAddress(org.dancres.paxos.impl.net.Utils.getBroadcastAddress(), 255);

        Thread myDispatcher = new Thread(this);

        myDispatcher.setDaemon(true);
        myDispatcher.start();

        _permuter = new Permuter<>(_env.getRng().nextLong());

        if (_env.isSimulating())
            _permuter.add(new PacketDrop()).add(new MachineBlip());
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

    Environment getEnv() {
        return _env;
    }

    Permuter<Context> getPermuter() {
        return _permuter;
    }

    Map<InetSocketAddress, OrderedMemoryTransportImpl> getTransports() {
        return new HashMap<>(_transports);
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