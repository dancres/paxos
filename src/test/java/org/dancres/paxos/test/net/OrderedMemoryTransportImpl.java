package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;

import org.dancres.paxos.impl.netty.PicklerImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.longterm.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class OrderedMemoryTransportImpl implements OrderedMemoryNetwork.OrderedMemoryTransport {
	static final Logger _logger = LoggerFactory.getLogger(OrderedMemoryTransportImpl.class);

	private final OrderedMemoryNetwork _parent;
	private final PacketPickler _pickler;
	private final Set<Dispatcher> _dispatcher = new HashSet<>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
    private final AtomicBoolean _dropRx = new AtomicBoolean((false));
    private final AtomicBoolean _dropTx = new AtomicBoolean((false));
	private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
    private final MessageBasedFailureDetector _fd;
    private Heartbeater _hb;

    @Override
    public void filterRx(Filter aFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void filterTx(Filter aFilter) {
        throw new UnsupportedOperationException();
    }

    /*
     Instantiate a Permuter with a context class defined to contain an instance of OrderedMemoryTransportImpl
     and Transport.Packet

     Add a Possibility innstance with a precondition that checks packet in context is not of type CLIENT
     If it isn't, assuming the RNG decides appropriately, the instance of OrderMemoryTransport in context is called
     to set an atomic boolean to drop next packet.

     Permuter::tick is called for each packet pass into send() or distribute() and, the flag is checked
     and reset to determine if packet is actually to be dropped and then acts accordingly.
     See the _decisions calls below for locations of that code.
     */
    public OrderedMemoryTransportImpl(InetSocketAddress aLocalAddr, InetSocketAddress aBroadAddr,
                                      OrderedMemoryNetwork aParent, MessageBasedFailureDetector anFD) {
        _unicastAddr = aLocalAddr;
        _broadcastAddr = aBroadAddr;
        _parent = aParent;
        _pickler = new PicklerImpl(_unicastAddr);
        _fd = anFD;

        if (_fd != null) {
            _hb = _fd.newHeartbeater(this, _unicastAddr.toString().getBytes());
            _hb.start();
        }
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

    public FailureDetector getFD() {
        return _fd;
    }

    List<Consumer<OrderedMemoryTransportImpl>> getDroppers() {
        return List.of(
                (t) -> t._dropRx.set(true),
                (t) -> t._dropTx.set(true),
                (t) -> {
                    t._dropRx.set(true);
                    t._dropTx.set(true);
                });
    }

    List<Consumer<OrderedMemoryTransportImpl>> getRestorers() {
        return List.of(
                (t) -> t._dropRx.set(false),
                (t) -> t._dropTx.set(false),
                (t) -> {
                    t._dropRx.set(false);
                    t._dropTx.set(false);
                });
    }

	private void guard() {
		if (_isStopping.get())
			throw new IllegalStateException("Transport is stopped");
	}
	
    public void routeTo(Dispatcher aDispatcher) {
    	guard();
    	
        synchronized(this) {
            _dispatcher.add(aDispatcher);
        }
    }

    Environment getEnv() {
        return _parent.getEnv();
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
		    _parent.getPermuter().tick(new OrderedMemoryNetwork.Context(aPacket, this));

		    if ((aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT)) ||
                    (! _dropTx.get()))
			    _parent.enqueue(aPacket, anAddr);
            else {
                _logger.warn("!!!!!!! OT [ " + getLocalAddress() + " ] DROPPED ON TXD: " + aPacket + " !!!!!!!");
            }
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
    }

    public void distribute(Transport.Packet aPacket) {
        _parent.getPermuter().tick(new OrderedMemoryNetwork.Context(aPacket, this));

        if ((aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT)) ||
                ! _dropRx.get()) {
            if ((_fd != null) && (_fd.accepts(aPacket))) {
                try {
                    _fd.processMessage(aPacket);
                } catch (Throwable aT) {
                    // Nothing to do
                }

                return;
            }

            synchronized(this) {
                for(Dispatcher d : _dispatcher)
                    d.packetReceived(aPacket);
            }
        } else {
            _logger.warn("!!!!!!! OT [ " + getLocalAddress() + " ] DROPPED ON RXD: " + aPacket + " !!!!!!!");
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

    public void settle() {
        _parent.getPermuter().restoreOutstanding(new OrderedMemoryNetwork.Context(null, this));
    }
}