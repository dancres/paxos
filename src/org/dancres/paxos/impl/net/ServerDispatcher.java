package org.dancres.paxos.impl.net;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metadata passed to the <code>ServerDispatcher</code> constructors will be advertised via Heartbeats.
 *
 * @see org.dancres.paxos.impl.faildet.Heartbeater
 */
public class ServerDispatcher implements Transport.Dispatcher, Listener {
    private static Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    private Core _core;
    private Transport _tp;

    private AtomicLong _handbackGenerator = new AtomicLong(0);
	private Map<String, InetSocketAddress> _requestMap = new ConcurrentHashMap<String, InetSocketAddress>();

    public ServerDispatcher(long anUnresponsivenessThreshold, byte[] aMeta) {
        this(anUnresponsivenessThreshold, new MemoryLogStorage(), aMeta);
    }

    public ServerDispatcher(long anUnresponsivenessThreshold) {
    	this(anUnresponsivenessThreshold, new MemoryLogStorage());
    }

    public ServerDispatcher(long anUnresponsivenessThreshold, LogStorage aLogger) {
        this(anUnresponsivenessThreshold, aLogger, null);
    }

    public ServerDispatcher(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta) {
        _core = new Core(anUnresponsivenessThreshold, aLogger, aMeta, this);
    }

    private class Core implements Transport.Dispatcher {
        private Transport _tp;
        private Listener _listener;
        private byte[] _meta = null;
        private AcceptorLearner _al;
        private Leader _ld;
        private FailureDetectorImpl _fd;
        private Heartbeater _hb;
        private long _unresponsivenessThreshold;
        private LogStorage _log;

        Core(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta, Listener aListener) {
            _meta = aMeta;
            _listener = aListener;
            _log = aLogger;
            _unresponsivenessThreshold = anUnresponsivenessThreshold;
        }

        void stop() {
            _fd.stop();
            _hb.halt();

            try {
                _hb.join();
            } catch (InterruptedException anIE) {
            }

            _tp.shutdown();
            _al.close();
            _ld.shutdown();
        }

        public void setTransport(Transport aTransport) throws Exception {
            _tp = aTransport;

            if (_meta == null)
                _hb = new Heartbeater(_tp, _tp.getLocalAddress().toString().getBytes());
            else
                _hb = new Heartbeater(_tp, _meta);

            _fd = new FailureDetectorImpl(_unresponsivenessThreshold);
            _al = new AcceptorLearner(_log, _fd, _tp);
            _al.open();
            _ld = new Leader(_fd, _tp, _al);
            _al.add(_listener);
            _hb.start();
        }

        FailureDetector getFailureDetector() {
            return _fd;
        }

        AcceptorLearner getAcceptorLearner() {
            return _al;
        }

        Leader getLeader() {
            return _ld;
        }


        public void messageReceived(PaxosMessage aMessage) {
            try {
                switch (aMessage.getClassification()) {
                    case PaxosMessage.FAILURE_DETECTOR : {
                        _fd.processMessage(aMessage);

                        break;
                    }

                    case PaxosMessage.LEADER:
                    case PaxosMessage.RECOVERY : {
                        _al.messageReceived(aMessage);

                        break;
                    }

                    case PaxosMessage.ACCEPTOR_LEARNER: {
                        _ld.messageReceived(aMessage);

                        break;
                    }

                    default : {
                        _logger.error("Unrecognised message:" + aMessage);
                    }
                }
            } catch (Exception anE) {
                _logger.error("Unexpected exception", anE);
            }
        }

        void submit(byte[] aValue, byte[] aHandback) {
            _ld.submit(aValue, aHandback);
        }
    }

    /**
     * @todo ServerDispatcher is currently playing several roles. It is the core routing function for the paxos
     * implementation and the server-side support for a basic client protocol (as seen with the handback logic
     * and dispatching of COMPLETE and FAIL messages). This results in conflating two separate sets of protocols on
     * top of one transport, an optimisation in respect of socket usage but it also ties together two codebases that
     * must be separated to provide a clean divide between client/server implementation and the paxos core.
     *
     * ServerDispatcher should thus be overhauled to reflect this split. We will introduce a nested class which will
     * also be a Transport.Dispatcher to hold all the core paxos routing logic. ServerDispatcher will handle the client
     * logic and invoke on the core logic to submit a vote request. It will also register a listener via core to process
     * framework responses. ServerDispatcher will be registered directly with the transport implementation whilst
     * the core will not be.
     *
     * Once this split is done, we refine Transport to accept multiple dispatchers routing packets to whichever will
     * accept the packet. This formally supports the transport sharing idea where both core and ServerDispatcher can
     * use the same infrastructure for messages. Transport can then be built up alongside core and made available to
     * ServerDispatcher via an appropriate call. Core will then be renamed Paxos.
     *
     * @param aMessage
     */
	public void messageReceived(PaxosMessage aMessage) {
		try {
			switch (aMessage.getClassification()) {
				case PaxosMessage.FAILURE_DETECTOR :
                case PaxosMessage.LEADER:
                case PaxosMessage.RECOVERY :
                case PaxosMessage.ACCEPTOR_LEARNER: {

                    _core.messageReceived(aMessage);

					break;
				}

				case PaxosMessage.CLIENT : {
                    String myHandback = Long.toString(_handbackGenerator.getAndIncrement());
                    _requestMap.put(myHandback, aMessage.getNodeId());

                    Post myPost = (Post) aMessage;
                    _core.submit(myPost.getValue(), myHandback.getBytes());

					break;	
				}


				default : {
					_logger.error("Unrecognised message:" + aMessage);
				}
			}
		} catch (Exception anE) {
        	_logger.error("Unexpected exception", anE);
        }
    }


	public void setTransport(Transport aTransport) throws Exception {
		_tp = aTransport;
        _core.setTransport(aTransport);
	}
	
	public Transport getTransport() {
		return _tp;
	}
	
	public FailureDetector getFailureDetector() {
		return _core.getFailureDetector();
	}
	
    public void stop() {
        _core.stop();
    }

    public void done(Event anEvent) {
        // If we're not the originating node for the post, because we're not leader, we won't have an address stored up
        //
        String myHandback = new String(anEvent.getHandback());
        InetSocketAddress myAddr = _requestMap.remove(myHandback);

        if (myAddr == null)
            return;

        if (anEvent.getResult() == Event.Reason.DECISION) {
            _tp.send(new Complete(anEvent.getSeqNum()), myAddr);
        } else {
            _tp.send(new Fail(anEvent.getSeqNum(), anEvent.getResult()), myAddr);
        }
    }

    public AcceptorLearner getAcceptorLearner() {
		return _core.getAcceptorLearner();
	}

	public Leader getLeader() {
		return _core.getLeader();
	}	
}
