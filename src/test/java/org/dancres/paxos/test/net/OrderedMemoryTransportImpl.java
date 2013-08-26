package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedMemoryTransportImpl implements OrderedMemoryNetwork.OrderedMemoryTransport {
	private static Logger _logger = LoggerFactory.getLogger(OrderedMemoryTransportImpl.class);

	private final OrderedMemoryNetwork _parent;
	private final PacketPickler _pickler;
	private final Set<Dispatcher> _dispatcher = new HashSet<Dispatcher>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
	private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
    private final RoutingDecisions _decisions;

    /**
     * A RoutingDecisions instance determines whether a network action should take place.
     */
    public interface RoutingDecisions {

        /**
         * Acceptable to send an unreliable packet?
         *
         * @return
         */
        boolean sendUnreliable();

        /**
         * Acceptable to receive a packet?
         *
         * @return
         */
        boolean receive();

        /**
         * Acceptable to send a reliable packet?
         *
         * @return
         */
        boolean sendReliable();

        /**
         * Acceptable to make a stream connection?
         *
         * @return
         */
        boolean connect();
    }

    private static class NullRoutingDecisionsImpl implements RoutingDecisions {
        public boolean sendUnreliable() {
            return true;
        }

        public boolean receive() {
            return true;
        }

        public boolean sendReliable() {
            return true;
        }

        public boolean connect() {
            return true;
        }
    }

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent, RoutingDecisions aDecisions) {
        _unicastAddr = aLocalAddr;
        _broadcastAddr = aBroadAddr;
        _parent = aParent;
        _decisions = aDecisions;
        _pickler = new StandalonePickler(_unicastAddr);
    }

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent) {
        this(aLocalAddr, aBroadAddr, aParent, new NullRoutingDecisionsImpl());
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

	private void guard() {
		if (_isStopping.get())
			throw new IllegalStateException("Transport is stopped");
	}
	
    public void routeTo(Dispatcher aDispatcher) throws Exception {
    	guard();
    	
        synchronized(this) {
            _dispatcher.add(aDispatcher);
        }
    }

	public InetSocketAddress getLocalAddress() {
		return _unicastAddr;
	}

    public InetSocketAddress getBroadcastAddress() {
    	return _broadcastAddr;
    }
	
    public void send(Packet aPacket, InetSocketAddress anAddr) {
		guard();
		
		try {
            if (_decisions.sendUnreliable())
			    _parent.enqueue(aPacket, anAddr);
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
    }

    public void distribute(Transport.Packet aPacket) {
        if (_decisions.receive())
            synchronized(this) {
                for(Dispatcher d : _dispatcher) {
                    if (d.messageReceived(aPacket))
                        break;
                }
            }
    }

    public void terminate() {
        guard();

		_isStopping.set(true);

        _parent.destroy(this);

        synchronized(this) {
            for (Dispatcher d: _dispatcher)
                try {
                    d.terminate();
                } catch (Exception anE) {
                    _logger.warn("Dispatcher didn't terminate cleanly", anE);
                }
        }
    }	
}