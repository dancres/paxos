package org.dancres.paxos.impl.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.*;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
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
@ChannelHandler.Sharable
public class TransportImpl extends SimpleChannelInboundHandler<DatagramPacket> implements Transport {
	private static final Logger _logger = LoggerFactory
			.getLogger(TransportImpl.class);

	private static final int BROADCAST_PORT = 41952;

    private volatile Heartbeater _hb;
    private final MessageBasedFailureDetector _fd;
    private final byte[] _meta;

    /**
     * Used by those requesting a senc that the message be given to all nodes.
     * This is effected by use of multicast over <code>_mcastAddr</code>
     */
    private final InetSocketAddress _internalAllNodesAddr =
            new InetSocketAddress(Utils.getBroadcastAddress(), 255);

    private final InetSocketAddress _unicastAddr;
	private final InetSocketAddress _mcastAddr;

	private final NioEventLoopGroup _multicastChannelLoop = new NioEventLoopGroup();
    private final NioEventLoopGroup _unicastChannelLoop = new NioEventLoopGroup();

	private final NioDatagramChannel _multicastChannel;
	private final NioDatagramChannel _unicastChannel;

    private final Set<Dispatcher> _dispatchers = new CopyOnWriteArraySet<>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
    private final PacketPickler _pickler = new PicklerImpl();
    private final List<Filter> _rxFilters = new LinkedList<>();
    private final List<Filter> _txFilters = new LinkedList<>();

    /**
     * Netty doesn't seem to like re-entrant behaviours so we need a thread pool
     *
     * TODO: Ought to be able to run this multi-threaded but AL is not ready for that yet
     */
    private final ExecutorService _packetDispatcher = Executors.newSingleThreadExecutor();

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

    public TransportImpl(MessageBasedFailureDetector anFD) throws Exception {
        this(anFD, null);
    }

    public TransportImpl(MessageBasedFailureDetector anFD, byte[] aMeta) throws Exception {
        this(new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 0), anFD, aMeta);
    }

    private TransportImpl(InetSocketAddress aServerAddr,
                         MessageBasedFailureDetector anFD, byte[] aMeta) throws Exception {
        _fd = anFD;

        _mcastAddr = new InetSocketAddress("224.0.0.1", BROADCAST_PORT);

        Bootstrap b = new Bootstrap()
                .group(_multicastChannelLoop)
                .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .localAddress(aServerAddr.getAddress(), _mcastAddr.getPort())
                .option(ChannelOption.IP_MULTICAST_IF, Utils.getWorkableInterface())
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(this);

        _multicastChannel = (NioDatagramChannel) b.bind(_mcastAddr.getPort()).sync().channel();
        _multicastChannel.joinGroup(_mcastAddr, Utils.getWorkableInterface()).sync();

        b = new Bootstrap()
                .group(_unicastChannelLoop)
                .channelFactory(() -> new NioDatagramChannel(InternetProtocolFamily.IPv4))
                .option(ChannelOption.SO_REUSEADDR, true)
                .handler(this);

        _unicastChannel = (NioDatagramChannel) b.bind(aServerAddr).sync().channel();
        _unicastAddr = _unicastChannel.localAddress();

        if (aMeta == null)
            _meta = _unicastAddr.toString().getBytes();
        else
            _meta = aMeta;

        if (_fd != null) {
            setupFailureDetector(_fd);
        }

        _logger.debug("Transport bound on: " + _unicastAddr);
    }

    private void setupFailureDetector(MessageBasedFailureDetector anFD) {
        /*
         * Activation of a heartbeater causes this transport to become visible to other cluster members
         * and clients. The result is it can "attract attention" before the node is fully initialised with
         * Paxos state such that it becomes disruptive. So we don't enable heartbeating until the FD is
         * properly initialised (pinned) with a membership.
         */
        anFD.addListener((FailureDetector aDetector, FailureDetector.State aState) -> {
            switch(aState) {
                case PINNED : {
                    if (_hb == null) {
                        _logger.debug("Activating Heartbeater");

                        _hb = anFD.newHeartbeater(TransportImpl.this, _meta);
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
        });
    }

    public PacketPickler getPickler() {
    	return _pickler;
    }

    @Override
    public void filterRx(Filter aFilter) {
        _rxFilters.add(aFilter);
    }

    @Override
    public void filterTx(Filter aFilter) {
        _txFilters.add(aFilter);
    }

    public FailureDetector getFD() {
        return _fd;
    }

	private void guard() {
		if (_isStopping.get())
			throw new IllegalStateException("Transport is stopped");
	}
	
    public void routeTo(Dispatcher aDispatcher) {
    	guard();

        _dispatchers.add(aDispatcher);
    }

    public void terminate() {
        _logger.debug("Terminate requested");
        
		if (! _isStopping.compareAndSet(false,true))
		    return;

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

			_logger.debug("Stop mcast factory");
			_multicastChannelLoop.shutdownGracefully(500,
                    500, TimeUnit.MILLISECONDS).sync();

			_logger.debug("Stop unicast factory");
			_unicastChannelLoop.shutdownGracefully(500,
                    500, TimeUnit.MILLISECONDS).sync();

            _logger.debug("Shutdown complete");
		} catch (Exception anE) {
			_logger.error("Failed to shutdown cleanly", anE);
		}

	}
	
	public InetSocketAddress getLocalAddress() {
		return _unicastAddr;
	}

    public InetSocketAddress getBroadcastAddress() {
		guard();

		return _internalAllNodesAddr;
    }
    
	public void channelActive(ChannelHandlerContext aContext) {
		_logger.debug("Connected: " + aContext);
	}

    public void channelInactive(ChannelHandlerContext aContext) {
        _logger.debug("Disconnected: " + aContext);
    }

    public void channelRead0(ChannelHandlerContext aContext, final DatagramPacket aPacket) {
		if (_isStopping.get())
			return;

		byte[] myBytes = new byte[aPacket.content().readableBytes()];

        aPacket.content().getBytes(0, myBytes);
        Packet myPacket = _pickler.unpickle(myBytes);

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
        _packetDispatcher.execute(() -> {
                for (Dispatcher d : _dispatchers)
                    try {
                        d.packetReceived(myFiltered);
                    } catch (Error anE) {
                        _logger.error("0!0!0!0!0!0! FATAL ISSUE - TERMINATING 0!0!0!0!0!0!", anE);
                        terminate();
                    }
            });
    }

    public void exceptionCaught(ChannelHandlerContext aContext, Throwable aThrowable) {
        _logger.error("Problem in transport", aThrowable);
        aContext.close();
    }		
	
	public void send(Packet aPacket, InetSocketAddress aNodeId) {
		guard();

		Packet myPacket = aPacket;
        for (Filter myFilter : _txFilters) {
            myPacket = myFilter.filter(this, myPacket);
            if (myPacket == null)
                return;
        }

		try {
            ByteBuf myBytes = Unpooled.copiedBuffer(_pickler.pickle(myPacket));

			if (aNodeId.equals(_internalAllNodesAddr))
				_multicastChannel.writeAndFlush(new DatagramPacket(myBytes, _mcastAddr));
			else {
				_unicastChannel.writeAndFlush(new DatagramPacket(myBytes, aNodeId));
			}
		} catch (Exception anE) {
			_logger.error("Failed to write message", anE);
		}
	}
}
