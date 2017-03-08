package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Paxos transport consists of two elements. The first is a shared multicast backplane that allows a node to efficiently
 * broadcast to all other nodes. This is typically used for failure detectors and leader to followers comms. The second
 * is a unicast connection used to talk to a specific node. This is typically used by followers wishing to respond
 * to a leader.
 *
 * The basic design assumptions for this transport are:
 *
 * <ol>
 * <li>Services that wish to base themselves upon Paxos consist of a client and a server component.</li>
 * <li>Transport between service server and client is independent of the Paxos transport.</li>
 * <li>Client sends a request to the server which is then sent to it's local leader.</li>
 * <li>Local leader will process the request and respond with success or failure.</li>
 * <li>Server acts upon the request and feeds back information to the client.</li>
 * </ol>
 * <ul>
 * <li>If the local leader returns other_leader the server should probably send a message to the client which
 * redirects it to the server node housing the current leader. This can be effected by having each server
 * advertise itself with two addresses. The first is designed for use by clients, the second would be the Paxos
 * nodeId for that server. Other servers could thus translate from the Paxos nodeId for another leader to the
 * real service address.</li>
 * <li>If the local leader returns a vote timeout, the server may choose to retry the operation submit a number of
 * times and if that fails redirect the client or report a disconnection.</li>
 * <li>The server is responsible for maintaining a list of active client requests and dispatching back to them
 * based on the responses from it's local leader and acceptorlearner.</li>
 * </ul>
 */
public class TransportImpl extends SimpleChannelHandler implements Transport {
	private static final Logger _logger = LoggerFactory
			.getLogger(TransportImpl.class);

	private static final int BROADCAST_PORT = 41952;

    private volatile Heartbeater _hb;
    private final MessageBasedFailureDetector _fd;
    private final byte[] _meta;
    private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
	private final InetSocketAddress _mcastAddr;

	private final DatagramChannelFactory _mcastFactory;
    private final DatagramChannelFactory _unicastFactory;

	private final DatagramChannel _mcast;
	private final DatagramChannel _unicast;

    private final ChannelGroup _channels = new DefaultChannelGroup();

    private final Set<Dispatcher> _dispatchers = new CopyOnWriteArraySet<>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
    private final PacketPickler _pickler = new PicklerImpl();
    private final List<Filter> _rxFilters = new LinkedList<>();

    /**
     * Netty doesn't seem to like re-entrant behaviours so we need a thread pool
     *
     * TODO: Ought to be able to run this multi-threaded but AL is not ready for that yet
     */
    private final ExecutorService _packetDispatcher = Executors.newSingleThreadExecutor();

    private static class Factory implements ThreadFactory {
		public Thread newThread(Runnable aRunnable) {
			Thread myThread = new Thread(aRunnable);

			myThread.setDaemon(true);
			return myThread;
		}
    }

    private class PicklerImpl implements PacketPickler {
        public Packet newPacket(PaxosMessage aMessage) {
            return new PacketImpl(aMessage, getLocalAddress());
        }

        public byte[] pickle(Packet aPacket) {
			byte[] myBytes = Codecs.encode(aPacket.getMessage());
			ByteBuffer myBuffer = ByteBuffer.allocate(8 + 4 + myBytes.length);

			myBuffer.putLong(Codecs.flatten(aPacket.getSource()));
			myBuffer.putInt(myBytes.length);
			myBuffer.put(myBytes);
			myBuffer.flip();

			return myBuffer.array();
        }

        public Packet unpickle(byte[] aBytes) {
			ByteBuffer myBuffer = ByteBuffer.wrap(aBytes);

			InetSocketAddress mySource = Codecs.expand(myBuffer.getLong());
			int myLength = myBuffer.getInt();
			byte[] myPaxosBytes = new byte[myLength];
			myBuffer.get(myPaxosBytes);

			PaxosMessage myMessage = Codecs.decode(myPaxosBytes);

			return new PacketImpl(myMessage, mySource);
        }
    }

    static {
    	System.runFinalizersOnExit(true);
    }

    private TransportImpl(PipelineFactory aFactory, MessageBasedFailureDetector anFD, byte[] aMeta) throws Exception {
        this(aFactory, new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 0), anFD, aMeta);
    }

    private TransportImpl(PipelineFactory aFactory, InetSocketAddress aServerAddr,
                         MessageBasedFailureDetector anFD, byte[] aMeta) throws Exception {
        if (aFactory == null)
            throw new IllegalArgumentException();

        PipelineFactory myFactory = aFactory;
        _fd = anFD;

        _mcastAddr = new InetSocketAddress("224.0.0.1", BROADCAST_PORT);
        _broadcastAddr = new InetSocketAddress(Utils.getBroadcastAddress(), 255);

        _mcastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(new Factory()));
        _mcast = _mcastFactory.newChannel(myFactory.newPipeline(_pickler, this));

        _mcast.getConfig().setReuseAddress(true);
        _mcast.bind(new InetSocketAddress(BROADCAST_PORT)).await();
        _mcast.joinGroup(_mcastAddr, Utils.getWorkableInterface()).await();
        _channels.add(_mcast);

        _unicastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(new Factory()));
        _unicast = _unicastFactory.newChannel(myFactory.newPipeline(_pickler, this));

        _unicast.getConfig().setReuseAddress(true);
        _unicast.bind(aServerAddr).await();
        _channels.add(_unicast);

        _unicastAddr = _unicast.getLocalAddress();

        if (aMeta == null)
            _meta = _unicastAddr.toString().getBytes();
        else
            _meta = aMeta;

        if (_fd != null) {
            /*
             * Activation of a heartbeater causes this transport to become visible to other cluster members
             * and clients. The result is it can "attract attention" before the node is fully initialised with
             * Paxos state such that it becomes disruptive. So we don't enable heartbeating until the FD is
             * properly initialised (pinned) with a membership.
             */
            _fd.addListener(new FailureDetector.StateListener() {
                                public void change(FailureDetector aDetector, FailureDetector.State aState) {
                                    switch(aState) {
                                        case PINNED : {
                                            if (_hb == null) {
                                                _logger.debug("Activating Heartbeater");

                                                _hb = _fd.newHeartbeater(TransportImpl.this, _meta);
                                                _hb.start();
                                            }

                                            break;
                                        }

                                        case STOPPED : {
                                            if (_hb != null) {
                                                _logger.debug("Deactivating Heartbeater");

                                                _hb.halt();

                                                try {
                                                    _hb.join();
                                                } catch (InterruptedException anIE) {
                                                }
                                            }

                                            break;
                                        }
                                    }
                                }
                            });
        }

        _logger.debug("Transport bound on: " + _unicastAddr);
    }

    public TransportImpl(MessageBasedFailureDetector anFD, byte[] aMeta) throws Exception {
        this(new DefaultPipelineFactory(), anFD, aMeta);
    }

	public TransportImpl(MessageBasedFailureDetector anFD) throws Exception {
        this(new DefaultPipelineFactory(), anFD, null);
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

    @Override
    public void add(Filter aFilter) {
        _rxFilters.add(aFilter);
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

        _dispatchers.add(aDispatcher);
    }

    public void terminate() {
        _logger.debug("Terminate requested");
        
		_isStopping.set(true);

        if (_fd != null)
            _fd.stop();

		_packetDispatcher.shutdown();

		try {
            for (Dispatcher d: _dispatchers)
                try {
                    d.terminate();
                } catch (Exception anE) {
                    _logger.warn("Dispatcher didn't terminate cleanly", anE);
                }

			_channels.close().await();
			
			_logger.debug("Stop mcast factory");
			_mcastFactory.releaseExternalResources();

			_logger.debug("Stop unicast factory");
			_unicastFactory.releaseExternalResources();

			_logger.debug("Shutdown complete");
		} catch (Exception anE) {
			_logger.error("Failed to shutdown cleanly", anE);
		}

	}
	
	protected void finalize() throws Throwable {
        super.finalize();

		if (! _isStopping.get())
			_logger.warn("Failed to close transport before JVM exit");
	}
	
	public InetSocketAddress getLocalAddress() {
		return _unicastAddr;
	}

    public InetSocketAddress getBroadcastAddress() {
		guard();

		return _broadcastAddr;
    }
    
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		_logger.debug("Connected: " + ctx + ", " + e);
		_channels.add(e.getChannel());
	}
	
	public void childChannelOpen(ChannelHandlerContext ctx, ChildChannelStateEvent e) throws Exception {
		_logger.debug("Stream open: " + ctx + ", " + e);
		_channels.add(e.getChannel());
	}
	
    public void messageReceived(ChannelHandlerContext aContext, final MessageEvent anEvent) {
		if (_isStopping.get())
			return;

        Packet myPacket = (Packet) anEvent.getMessage();

        for (Filter myFilter : _rxFilters) {
            myPacket = myFilter.filter(this, myPacket);
            if (myPacket == null)
                return;
        }

        if ((_fd != null) && (_fd.accepts(myPacket))) {
            try {
            _fd.processMessage(myPacket);
            } catch (Throwable aT) {
                // Nothing to do
            }

            return;
        }

        final Packet myFiltered = myPacket;
        _packetDispatcher.execute(new Runnable() {
            public void run() {
                for (Dispatcher d : _dispatchers) {
                    if (d.packetReceived(myFiltered))
                        break;
                }
            }
        });
    }

    public void exceptionCaught(ChannelHandlerContext aContext, ExceptionEvent anEvent) {
        _logger.error("Problem in transport", anEvent.getCause());
        // anEvent.getChannel().close();
    }		
	
	public void send(Packet aPacket, InetSocketAddress aNodeId) {
		guard();
		
		try {
			if (aNodeId.equals(_broadcastAddr))
				_mcast.write(aPacket, _mcastAddr);
			else {
				_unicast.write(aPacket, aNodeId);
			}
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
	}
}
