package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Stream;
import org.dancres.paxos.messages.PaxosMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.atomic.AtomicBoolean;

public class OrderedMemoryTransportImpl implements OrderedMemoryNetwork.OrderedMemoryTransport {
	private static Logger _logger = LoggerFactory.getLogger(OrderedMemoryTransportImpl.class);

	private OrderedMemoryNetwork _parent;
	private PacketPickler _pickler = new StandalonePickler();
	private Set<Dispatcher> _dispatcher = new HashSet<Dispatcher>();
    private AtomicBoolean _isStopping = new AtomicBoolean(false);
	private InetSocketAddress _unicastAddr;
    private InetSocketAddress _broadcastAddr;

    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr, OrderedMemoryNetwork aParent) {
    	_unicastAddr = aLocalAddr;
    	_broadcastAddr = aBroadAddr;
    	_parent = aParent;
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

	private void guard() {
		if (_isStopping.get())
			throw new IllegalStateException("Transport is stopped");
	}
	
    public void add(Dispatcher aDispatcher) throws Exception {
    	guard();
    	
        synchronized(this) {
            _dispatcher.add(aDispatcher);
            aDispatcher.setTransport(this);
        }
    }

	public InetSocketAddress getLocalAddress() {
		return _unicastAddr;
	}

    public InetSocketAddress getBroadcastAddress() {
    	return _broadcastAddr;
    }
	
    /**
     * @param aMessage is the message to send
     * @param anAddr is the address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, InetSocketAddress anAddr) {
		guard();
		
		try {
			_parent.enqueue(new FakePacket(_unicastAddr, aMessage), anAddr);
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
    }

    public void distribute(Transport.Packet aPacket) {
    	synchronized(this) {
            for(Dispatcher d : _dispatcher) {
                if (d.messageReceived(aPacket))
                    break;
            }
    	}    	
    }

	private class StreamImpl implements Stream {
		private InetSocketAddress _target;
		private AtomicBoolean _closed = new AtomicBoolean(false);

		StreamImpl(InetSocketAddress aTarget) {
			_target = aTarget;
		}
		
		public void close() {
			_closed.set(true);
		}
		
		public void send(PaxosMessage aMessage) {
			if (_closed.get())
				throw new RuntimeException("Stream is closed");

			try {
				_parent.enqueue(new FakePacket(_unicastAddr, aMessage), _target);
			} catch (Exception anE) {
				_logger.error("Couldn't enqueue packet", anE);
			}
		}

		public void sendRaw(Packet aPacket) {
			if (_closed.get())
				throw new RuntimeException("Stream is closed");

			try {
				_parent.enqueue(aPacket, _target);
			} catch (Exception anE) {
				_logger.error("Couldn't enqueue packet", anE);
			}
		}
	}
	
	public void connectTo(final InetSocketAddress aNodeId, final ConnectionHandler aHandler) {
		guard();
		
        aHandler.connected(new StreamImpl(aNodeId));
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