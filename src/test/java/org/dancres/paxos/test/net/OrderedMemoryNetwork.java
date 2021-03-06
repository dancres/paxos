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
import java.util.function.BiFunction;

public class OrderedMemoryNetwork implements Runnable {
    private static final Logger _logger = LoggerFactory.getLogger(OrderedMemoryNetwork.class);

    public interface OrderedMemoryTransport extends Transport {
        void distribute(Transport.Packet aPacket);
        void settle();
    }

    public static class Context {
        final Transport.Packet _packet;
        final OrderedMemoryTransportImpl _transport;

        Context(Transport.Packet aPacket, OrderedMemoryTransportImpl aTransport) {
            _packet = aPacket;
            _transport = aTransport;
        }
    }

    public interface TransportIntegrator extends
            BiFunction<OrderedMemoryTransport, Object, Constructed> {
    }
    
    public static class Constructed {
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

    private class PacketWrapper {
        private final Transport.Packet _packet;
        private final InetSocketAddress _target;

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

    private final BlockingQueue<PacketWrapper> _queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
    private final InetSocketAddress  _broadcastAddr;
    private final Map<InetSocketAddress, OrderedMemoryTransportImpl> _transports =
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

    public Constructed newTransport(TransportIntegrator aFunction,
                                                     MessageBasedFailureDetector anFD, InetSocketAddress anAddr,
                                                     Object aContext) {
        OrderedMemoryTransportImpl myTransport =
                new OrderedMemoryTransportImpl(anAddr, _broadcastAddr, this, anFD);

        Constructed myResult = aFunction.apply(myTransport, aContext);

        _transports.put(anAddr, myTransport);

        return myResult;
    }

    void destroy(OrderedMemoryTransport aTransport) {
        _transports.remove(aTransport.getLocalAddress());
    }
}