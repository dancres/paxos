package org.dancres.paxos.impl.core;

import java.net.SocketAddress;
import org.dancres.paxos.impl.faildet.Membership;
import org.dancres.paxos.impl.faildet.MembershipListener;
import org.dancres.paxos.impl.core.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Responsible for attempting to drive consensus for a particular entry in the paxos ledger (as identified by a sequence number)
 * @author dan
 */
class LeaderImpl implements MembershipListener {
    /*
     * Used to compute the timeout period for watchdog tasks.  In order to behave sanely we want the failure detector to be given
     * the best possible chance of detecting problems with the members.  Thus the timeout for the watchdog is computed as the
     * unresponsiveness threshold of the failure detector plus a grace period.
     */
    private static final long FAILURE_DETECTOR_GRACE_PERIOD = 2000;

    private static final int COLLECT = 0;
    private static final int BEGIN = 1;
    private static final int SUCCESS = 2;
    private static final int EXIT = 3;
    private static final int ABORT = 4;

    private static Timer _watchdog = new Timer("Leader watchdog");

    private long _watchdogTimeout;
    private long _seqNum;
    private byte[] _value;

    // Broadcast channel for all acceptor/learners
    //
    private Channel _channel;
    private Channel _clientChannel;

    private TimerTask _activeAlarm;

    private long _rndNumber = 0;

    private ProposerState _state;
    private Membership _membership;

    private int _stage = COLLECT;

    private List _messages = new ArrayList();

    private Logger _logger = LoggerFactory.getLogger(LeaderImpl.class);

    /**
     * @param aSeqNum is the sequence number for the proposal this leader instance is responsible for
     * @param aProposerState is the proposer state to use for this proposal
     * @param aBroadcastChannel is the channel on which acceptor/learners can be reached
     * @param aClientChannel is the channel on which the client that started this proposal can be found
     */
    LeaderImpl(long aSeqNum, ProposerState aProposerState, Channel aBroadcastChannel, Channel aClientChannel) {
        _seqNum = aSeqNum;
        _state = aProposerState;
        _channel = aBroadcastChannel;
        _clientChannel = aClientChannel;
        _watchdogTimeout = _state.getFailureDetector().getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;
    }

    /**
     * @todo If we want to retry in face of ABORT we'd reacquire a membership, increment a retry count etc
     * @todo Send client a failure message
     */
    private void process() {
        switch(_stage) {
            case ABORT :
            case EXIT : {
                _logger.info("Exiting leader: " + _seqNum + " " + (_stage == EXIT));

                _membership.dispose();
                _state.dispose(_seqNum);

                if (_stage == EXIT) {
                    _clientChannel.write(new Ack(_seqNum));
                } else {
                    _clientChannel.write(new Fail(_seqNum));
                }

                return;
            }

            case COLLECT : {
                collect();
                break;
            }

            case BEGIN : {
                // Process _messages to assess what we do next - might be to launch a new round or to give up
                // We announced round 0, and so if responses are for round 0 we want our value to take precedent
                // thus the minimum round that can be accepted to overrule our value is 1.
                //
                long myMaxRound = 0;
                byte[] myValue = _value;

                Iterator myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = (PaxosMessage) myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        OldRound myOldRound = (OldRound) myMessage;

                        _rndNumber = myOldRound.getLastRound() + 1;

                        _stage = COLLECT;
                        collect();

                        return;
                    } else {
                        Last myLast = (Last) myMessage;

                        if (myLast.getRndNumber() > myMaxRound) {
                            myMaxRound = myLast.getRndNumber();
                            myValue = myLast.getValue();
                        }
                    }
                }

                _value = myValue;
                begin();

                break;
            }

            case SUCCESS : {

                // Old round message, causes start at collect
                // If Accept messages total more than majority we're happy, send Success wait for all acks
                // or redo collect
                //
                int myAcceptCount = 0;

                Iterator myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = (PaxosMessage) myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        OldRound myOldRound = (OldRound) myMessage;

                        _rndNumber = myOldRound.getLastRound() + 1;

                        _stage = COLLECT;
                        collect();

                        return;
                    } else {
                        myAcceptCount++;
                    }
                }

                if (myAcceptCount >= _membership.getMajority()) {
                    // Send success, wait for acks
                    success();
                } else {
                    // Need another round then.....
                    _rndNumber += 1;

                    _stage = COLLECT;
                    collect();
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _stage);
        }
    }

    private void collect() {
        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Collect(_seqNum, _rndNumber, _state.getNodeId()),
                _state.getAddress().getPort());

        startInteraction();

        _logger.info("Leader sending collect: " + _seqNum);

        _channel.write(myMessage);
    }

    private void begin() {
        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Begin(_seqNum, _rndNumber, _state.getNodeId(), _value),
                _state.getAddress().getPort());

        startInteraction();

        _logger.info("Leader sending begin: " + _seqNum);

        _channel.write(myMessage);
    }

    private void success() {
        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Success(_seqNum, _value), _state.getAddress().getPort());

        startInteraction();

        _logger.info("Leader sending success: " + _seqNum);

        _channel.write(myMessage);
    }

    private void startInteraction() {
        _activeAlarm = new Alarm();
        _watchdog.schedule(_activeAlarm, _watchdogTimeout);

        _membership.startInteraction();
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        _logger.info("Membership requested abort: " + _seqNum);

        _activeAlarm.cancel();

        synchronized(this) {
            _stage = ABORT;
            process();
        }
    }

    public void allReceived() {
        _activeAlarm.cancel();

        synchronized(this) {
            _stage++;
            process();
        }
    }

    private void expired() {
        _logger.info("Watchdog requested abort: " + _seqNum);

        synchronized(this) {
            _stage = ABORT;
            process();
        }
    }

    /**
     * @todo Failure detector my not catch a blip in partition which would cause a single message to be lost, add a
     * timeout to cope with loss or do packet resends
     */
    void messageReceived(PaxosMessage aMessage, SocketAddress anAddress) {
        _logger.info("Leader received message: " + aMessage);

        if (aMessage.getType() == Operations.POST) {
            _logger.info("Initialising leader: " + _seqNum);

            // Send a collect message
            _membership = _state.getFailureDetector().getMembers(this);

            _value = ((Post) aMessage).getValue();

            _logger.info("Got membership for leader: " + _seqNum + ", (" + _membership.getSize() + ")");

            synchronized(this) {
                process();
            }
        } else {
            synchronized(this) {
                _messages.add(aMessage);
                _membership.receivedResponse(anAddress);
            }
        }

        _logger.info("Leader processed message: " + aMessage);
    }

    private class Alarm extends TimerTask {
        public void run() {
            expired();
        }
    }
}
