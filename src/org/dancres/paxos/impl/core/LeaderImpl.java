package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.Membership;
import org.dancres.paxos.impl.core.MembershipListener;
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
    // private Channel _channel;
    // private Channel _clientChannel;

    private Transport _transport;
    private Address _clientAddress;

    private TimerTask _activeAlarm;

    private ProposerState _state;
    private Membership _membership;

    private int _stage = COLLECT;
    
    /**
     * In cases of ABORT, indicates the reason
     */
    private int _reason = 0;

    private List _messages = new ArrayList();

    private Logger _logger = LoggerFactory.getLogger(LeaderImpl.class);

    /**
     * @param aSeqNum is the sequence number for the proposal this leader instance is responsible for
     * @param aProposerState is the proposer state to use for this proposal
     * @param aTransport is the transport to use for messages
     * @param aClientAddress is the endpoint for the client
     */
    LeaderImpl(long aSeqNum, ProposerState aProposerState, Transport aTransport, Address aClientAddress) {
        _seqNum = aSeqNum;
        _state = aProposerState;
        _transport = aTransport;
        _clientAddress = aClientAddress;
        _watchdogTimeout = _state.getFailureDetector().getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;

        if (_state.isLeader())
            _stage = BEGIN;
        else
            _stage = COLLECT;
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we process the
     * result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state we expect ACCEPT or OLDROUND
     *
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
                    _transport.send(new Ack(_seqNum), _clientAddress);
                } else {
                    _transport.send(new Fail(_seqNum, _reason), _clientAddress);
                }

                return;
            }

            case COLLECT : {
                collect();
                break;
            }

            case BEGIN : {
                byte[] myValue = _value;

                // If we're not currently the leader, we'll have issued a collect and must process the responses
                //
                if (! _state.isLeader()) {
                    // Process _messages to assess what we do next - might be to launch a new round or to give up
                    //
                    long myMaxRound = 0;
                    Iterator myMessages = _messages.iterator();

                    while (myMessages.hasNext()) {
                        PaxosMessage myMessage = (PaxosMessage) myMessages.next();

                        if (myMessage.getType() == Operations.OLDROUND) {
                            oldRound(myMessage);
                            return;
                        } else {
                            Last myLast = (Last) myMessage;

                            if (myLast.getRndNumber() > myMaxRound) {
                                myMaxRound = myLast.getRndNumber();
                                myValue = myLast.getValue();
                            }
                        }
                    }
                }

                _state.amLeader();
                _value = myValue;
                begin();

                break;
            }

            case SUCCESS : {

                /*
                 * Old round message, causes start at collect or quit.
                 * If Accept messages total more than majority we're happy, send Success wait for all acks
                 * or redo collect
                 */
                int myAcceptCount = 0;

                Iterator myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = (PaxosMessage) myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        oldRound(myMessage);
                        return;
                    } else {
                        myAcceptCount++;
                    }
                }

                if (myAcceptCount >= _membership.getMajority()) {
                    // Send success, wait for acks
                    //
                    success();
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    _stage = BEGIN;
                    begin();
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _stage);
        }
    }

    /**
     * @todo Check the failure detector before deciding to abort - if the other leader is dead, we'll continue
     * in spite of the OLDROUND which is potentially out-of-date.
     *
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        long myCompetingNodeId = myOldRound.getNodeId();

        /*
         * Some other node is active, we should abort if they are the leader by virtue of a larger nodeId
         */
        if (myCompetingNodeId > _state.getNodeId()) {
            _logger.info("Superior leader is active, backing down: " + Long.toHexString(myCompetingNodeId) + ", " +
                    Long.toHexString(_state.getNodeId()));

            _state.notLeader();
            _stage = ABORT;
            _reason = Reasons.OTHER_LEADER;
            process();
            return;
        }

        _state.updateRndNumber(myOldRound);

        /*
         * Some other leader is active but we are superior, restart negotiations with COLLECT
         */
        _stage = COLLECT;
        collect();
    }

    private void collect() {
        _messages.clear();

        PaxosMessage myMessage = new Collect(_seqNum, _state.newRndNumber(), _state.getNodeId());

        startInteraction();

        _logger.info("Leader sending collect: " + _seqNum);

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void begin() {
        _messages.clear();

        PaxosMessage myMessage = new Begin(_seqNum, _state.getRndNumber(), _state.getNodeId(), _value);

        startInteraction();

        _logger.info("Leader sending begin: " + _seqNum);

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void success() {
        _messages.clear();

        PaxosMessage myMessage = new Success(_seqNum, _value);

        startInteraction();

        _logger.info("Leader sending success: " + _seqNum);

        _transport.send(myMessage, Address.BROADCAST);
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
            _reason = Reasons.BAD_MEMBERSHIP;
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
            _reason = Reasons.VOTE_TIMEOUT;
            process();
        }
    }

    /**
     * @todo Failure detector my not catch a blip in partition which would cause a single message to be lost, add a
     * timeout to cope with loss or do packet resends
     */
    void messageReceived(PaxosMessage aMessage, Address anAddress) {
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
