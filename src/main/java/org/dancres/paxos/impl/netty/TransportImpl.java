package org.dancres.paxos.impl.netty;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
 * </ol>
 */
public class TransportImpl extends SimpleChannelHandler implements Transport {
	private static final Logger _logger = LoggerFactory
			.getLogger(TransportImpl.class);

	private static final int BROADCAST_PORT = 41952;

    private final InetSocketAddress _unicastAddr;
    private final InetSocketAddress _broadcastAddr;
	private final InetSocketAddress _mcastAddr;

    private final NioServerSocketChannelFactory _serverStreamFactory;
    private final NioClientSocketChannelFactory _clientStreamFactory;
	private final DatagramChannelFactory _mcastFactory;
    private final DatagramChannelFactory _unicastFactory;

	private final DatagramChannel _mcast;
	private final DatagramChannel _unicast;

    private final ChannelGroup _channels = new DefaultChannelGroup();

    private final Set<Dispatcher> _dispatchers = new CopyOnWriteArraySet<Dispatcher>();
    private final AtomicBoolean _isStopping = new AtomicBoolean(false);
    private final PacketPickler _pickler = new PicklerImpl();
    private final PipelineFactory _pipelineFactory;

    /**
     * Netty doesn't seem to like re-entrant behaviours so we need a thread pool
     *
     * @todo Ought to be able to run this multi-threaded but AL is not ready for that yet
     */
    private final ExecutorService _packetDispatcher = Executors.newSingleThreadExecutor();
	
    private class Factory implements ThreadFactory {
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

    public TransportImpl(PipelineFactory aFactory) throws Exception {
        if (aFactory == null)
            throw new IllegalArgumentException();

        _pipelineFactory = aFactory;

        _mcastAddr = new InetSocketAddress("224.0.0.1", BROADCAST_PORT);
        _broadcastAddr = new InetSocketAddress(Utils.getBroadcastAddress(), 255);

        InetSocketAddress myMcastTarget = new InetSocketAddress((InetAddress) null,
                BROADCAST_PORT);

        _mcastFactory = new OioDatagramChannelFactory(Executors.newCachedThreadPool(new Factory()));

        _mcast = _mcastFactory.newChannel(_pipelineFactory.newPipeline(_pickler, this));
        _mcast.bind(myMcastTarget).await();
        _mcast.joinGroup(_mcastAddr.getAddress()).await();
        _channels.add(_mcast);

        _unicastFactory = new NioDatagramChannelFactory(Executors.newCachedThreadPool(new Factory()));
        _unicast = _unicastFactory.newChannel(_pipelineFactory.newPipeline(_pickler, this));
        _unicast.bind(new InetSocketAddress(Utils.getWorkableInterface(), 0)).await();
        _channels.add(_unicast);

        _unicastAddr = _unicast.getLocalAddress();

        _logger.debug("Transport bound on: " + _unicastAddr);

        _serverStreamFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new Factory()),
                Executors.newCachedThreadPool());
        ServerSocketChannel myStreamChannel = _serverStreamFactory.newChannel(_pipelineFactory.newPipeline(_pickler, this));
        myStreamChannel.bind(_unicast.getLocalAddress()).await();
        myStreamChannel.getConfig().setPipelineFactory(
                Channels.pipelineFactory(_pipelineFactory.newPipeline(_pickler, this)));
        _channels.add(myStreamChannel);

        _clientStreamFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(new Factory()),
                Executors.newCachedThreadPool());
    }

	public TransportImpl() throws Exception {
        this(new DefaultPipelineFactory());
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

        _dispatchers.add(aDispatcher);
    }

    public void terminate() {
		_isStopping.set(true);
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

			_logger.debug("Stop serverstream factory");
			_serverStreamFactory.releaseExternalResources();
			
			_logger.debug("Stop clientstream factory");
			_clientStreamFactory.releaseExternalResources();			
			
			_logger.debug("Shutdown complete");
		} catch (Exception anE) {
			_logger.error("Failed to shutdown cleanly", anE);
		}

	}
	
	protected void finalize() throws Throwable {
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

        _packetDispatcher.execute(new Runnable() {
            public void run() {
                for(Dispatcher d : _dispatchers) {
                    if (d.messageReceived((Packet) anEvent.getMessage()))
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

	private class StreamImpl implements Stream {
		private final SocketChannel _channel;
		
		StreamImpl(SocketChannel aChannel) {
			_channel = aChannel;
		}
		
		public void close() {
			try {
				_channels.remove(_channel);
				_channel.close().await();
			} catch (InterruptedException anIE) {
			}
		}
		
		public void send(Packet aPacket) {
			try {
				_channel.write(aPacket).await();
			} catch (InterruptedException anIE) {				
			}
		}
	}
	
	public void connectTo(final InetSocketAddress aNodeId, final ConnectionHandler aHandler) {
		guard();
		
		final SocketChannel myChannel = _clientStreamFactory.newChannel(_pipelineFactory.newPipeline(_pickler, this));

        myChannel.connect(aNodeId).addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture aFuture) throws Exception {
                if (aFuture.isSuccess()) {
                    _channels.add(myChannel);

                    aHandler.connected(new StreamImpl(myChannel));
                } else {
                    _logger.error("Couldn't connect to: " + aNodeId, aFuture.getCause());
                }
            }
        });
	}
	
	public static void main(String[] anArgs) throws Exception {
		Transport _tport1 = new TransportImpl();
        _tport1.routeTo(new DispatcherImpl());

		Transport _tport2 = new TransportImpl();
        _tport2.routeTo(new DispatcherImpl());
		
		_tport1.send(_tport1.getPickler().newPacket(new Accept(1, 2)), _tport1.getBroadcastAddress());
		_tport1.send(_tport1.getPickler().newPacket(new Accept(2, 3)), _tport2.getLocalAddress());

		Thread.sleep(5000);
		_tport1.terminate();
		_tport2.terminate();
	}
	
	static class DispatcherImpl implements Dispatcher {
		public boolean messageReceived(Packet aPacket) {
			System.err.println("Message received: " + aPacket.getMessage());

            return true;
		}

		public void init(Transport aTransport) {
			System.err.println("Dispatcher " + this + " got transport: " + aTransport);
		}

        public void terminate() {
            System.err.println("Dispatcher " + this + " got terminate");
        }
	}
}
