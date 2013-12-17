package org.dancres.paxos.test.net;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;

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
    private final MessageBasedFailureDetector _fd = new FailureDetectorImpl(5, 5000);
	private final Set<Dispatcher> _dispatcher = new HashSet<Dispatcher>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
	private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
    private final RoutingDecisions _decisions;
    private final Heartbeater _hb;

    /**
     * A RoutingDecisions instance determines whether a network action should take place.
     * This is typically used for failure testing etc.
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
    }

    private static class NullRoutingDecisionsImpl implements RoutingDecisions {
        public boolean sendUnreliable() {
            return true;
        }

        public boolean receive() {
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
        _hb = _fd.newHeartbeater(this, _unicastAddr.toString().getBytes());
        _hb.start();
    }

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent) {
        this(aLocalAddr, aBroadAddr, aParent, new NullRoutingDecisionsImpl());
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

    public FailureDetector getFD() {
        return _fd;
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
        if (_decisions.receive()) {
            if ((_fd != null) && (_fd.accepts(aPacket))) {
                try {
                    _fd.processMessage(aPacket);
                } catch (Throwable aT) {
                    // Nothing to do
                }

                return;
            }

            synchronized(this) {
                for(Dispatcher d : _dispatcher) {
                    if (d.messageReceived(aPacket))
                        break;
                }
            }
        }
    }

    public void terminate() {
        guard();

        _hb.halt();

        try {
            _hb.join();
        } catch (InterruptedException anIE) {
        }

        _fd.stop();

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