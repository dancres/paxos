package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;

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
	private final Set<Dispatcher> _dispatcher = new HashSet<>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
	private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
    private final RoutingDecisions _decisions;
    private MessageBasedFailureDetector _fd;
    private Heartbeater _hb;

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
        boolean sendUnreliable(Packet aPacket);

        /**
         * Acceptable to receive a packet?
         *
         * @return
         */
        boolean receive(Packet aPacket);
    }

    @Override
    public void add(Filter aFilter) {
        throw new UnsupportedOperationException();
    }

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent, MessageBasedFailureDetector anFD,
                                      RoutingDecisions aDecisions) {
        _unicastAddr = aLocalAddr;
        _broadcastAddr = aBroadAddr;
        _parent = aParent;
        _decisions = aDecisions;
        _pickler = new StandalonePickler(_unicastAddr);
        _fd = anFD;

        if (_fd != null) {
            _hb = _fd.newHeartbeater(this, _unicastAddr.toString().getBytes());
            _hb.start();
        }
    }

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent, MessageBasedFailureDetector anFD) {
        this(aLocalAddr, aBroadAddr, aParent, anFD, new RoutingDecisions() {
            public boolean sendUnreliable(Packet aPacket) {
                return true;
            }

            public boolean receive(Packet aPacket) {
                return true;
            }
        });
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
            if (_decisions.sendUnreliable(aPacket))
			    _parent.enqueue(aPacket, anAddr);
            else {
                _logger.warn("OT [ " + getLocalAddress() + " ] dropped on txd: " + aPacket);
            }
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
    }

    public void distribute(Transport.Packet aPacket) {
        if (_decisions.receive(aPacket)) {
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
                    if (d.packetReceived(aPacket))
                        break;
                }
            }
        } else {
            _logger.warn("OT [ " + getLocalAddress() + " ] dropped on rxd: " + aPacket);
        }
    }

    public void terminate() {
        guard();

        _isStopping.set(true);


        if (_fd != null) {
            _hb.halt();

            try {
                _hb.join();
            } catch (InterruptedException anIE) {
            }

            _fd.stop();
        }

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