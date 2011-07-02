package org.dancres.paxos.impl;

import org.dancres.paxos.Paxos;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Core implements Transport.Dispatcher {
    private static Logger _logger = LoggerFactory.getLogger(Core.class);

    private Transport _tp;
    private Paxos.Listener _listener;
    private byte[] _meta = null;
    private AcceptorLearner _al;
    private Leader _ld;
    private FailureDetectorImpl _fd;
    private Heartbeater _hb;
    private long _unresponsivenessThreshold;
    private LogStorage _log;

    public Core(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta,
                Paxos.Listener aListener) {
        _meta = aMeta;
        _listener = aListener;
        _log = aLogger;
        _unresponsivenessThreshold = anUnresponsivenessThreshold;
    }

    public void stop() {
        _ld.shutdown();

        _fd.stop();
        _hb.halt();

        try {
            _hb.join();
        } catch (InterruptedException anIE) {
        }

        _tp.shutdown();
        _al.close();
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

    public FailureDetector getFailureDetector() {
        return _fd;
    }

    public AcceptorLearner getAcceptorLearner() {
        return _al;
    }

    public Leader getLeader() {
        return _ld;
    }


    public boolean messageReceived(PaxosMessage aMessage) {
        try {
            switch (aMessage.getClassification()) {
                case PaxosMessage.FAILURE_DETECTOR: {
                    _fd.processMessage(aMessage);

                    return true;
                }

                case PaxosMessage.LEADER:
                case PaxosMessage.RECOVERY: {
                    _al.messageReceived(aMessage);

                    return true;
                }

                case PaxosMessage.ACCEPTOR_LEARNER: {
                    _ld.messageReceived(aMessage);

                    return true;
                }

                default: {
                    _logger.debug("Unrecognised message:" + aMessage);
                    return false;
                }
            }
        } catch (Exception anE) {
            _logger.error("Unexpected exception", anE);
            return false;
        }
    }

    public void submit(byte[] aValue, byte[] aHandback) {
        _ld.submit(aValue, aHandback);
    }
}

