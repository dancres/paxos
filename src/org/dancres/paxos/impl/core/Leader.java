package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the leader state machine.
 *
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

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

    /**
     * Indicates the Leader is not ready to process the passed message and the caller should retry.
     */
    public static final int BUSY = 256;

    /**
     * Indicates the Leader has accepted the message and is waiting for further messages.
     */
    public static final int CONTINUE = 257;

    private final Timer _watchdog = new Timer("Leader watchdog");
    private final long _watchdogTimeout;
    private final FailureDetector _detector;
    private final long _nodeId;
    private final Transport _transport;

    private final List<LeaderListener> _listeners = new ArrayList<LeaderListener>();

    private long _seqNum = LogStorage.EMPTY_LOG;
    private long _rndNumber = 0;
    private byte[] _value;

    /**
     * Note that being the leader is merely an optimisation and saves on sending COLLECTs.  Thus if one thread establishes we're leader and
     * a prior thread decides otherwise with the latter being last to update this variable we'll simply do an unnecessary COLLECT.  The
     * protocol execution will still be correct.
     */
    private boolean _isLeader = false;

    /**
     * Maintains the current client request. The actual sequence number and value the state machine operates on are held in
     * <code>_seqNum</code> and <code>_value</code> and during recovery will not be the same as the client request.  Thus we cache
     * the client request and move it into the operating variables once recovery is complete.
     */
    private Post _clientPost;
    private Address _clientAddress;

    private TimerTask _activeAlarm;

    private Membership _membership;

    private int _stage = EXIT;
    
    /**
     * In cases of ABORT, indicates the reason
     */
    private int _reason = 0;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    /**
     * @param aSeqNum is the sequence number for the proposal this leader instance is responsible for
     * @param aProposerState is the proposer state to use for this proposal
     * @param aTransport is the transport to use for messages
     * @param aClientAddress is the endpoint for the client
     */
    public Leader(FailureDetector aDetector, long aNodeId, Transport aTransport) {
        _nodeId = aNodeId;
        _detector = aDetector;
        _transport = aTransport;
        _watchdogTimeout = _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;
    }

    public void add(LeaderListener aListener) {
        synchronized(_listeners) {
            _listeners.add(aListener);
        }
    }

    public void remove(LeaderListener aListener) {
        synchronized(_listeners) {
            _listeners.remove(aListener);
        }
    }

    private long newRndNumber() {
        synchronized(this) {
            return ++_rndNumber;
        }
    }

    private void updateRndNumber(OldRound anOldRound) {
        synchronized(this) {
            _rndNumber = anOldRound.getLastRound() + 1;
        }
    }

    private boolean isLeader() {
        synchronized(this) {
            return _isLeader;
        }
    }

    private void amLeader() {
        synchronized(this) {
            _isLeader = true;
        }
    }

    private void notLeader() {
        synchronized(this) {
            _isLeader = false;
        }
    }

    private long getRndNumber() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    public long getNodeId() {
        return _nodeId;
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we process the
     * result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state we expect ACCEPT or OLDROUND
     *
     * @todo Check Last messages to compute minimum low and maximum high watermarks, then use them to perform recovery
     * @todo Ensure during recovery that BEGIN doesn't screw up the sequence number - perhaps during recovery prevent BEGIN from doing
     * sequence number increments
     * @todo Handle all cases in SUCCEESS e.g. we don't properly process/wait for all Acks
     */
    private void process() {
        switch(_stage) {
            case ABORT :
            case EXIT : {
                _logger.info("Exiting leader: " + Long.toHexString(_seqNum) + " " + (_stage == EXIT));

                if (_stage == EXIT) {
                    _transport.send(new Ack(_seqNum), _clientAddress);
                } else {
                    _transport.send(new Fail(_seqNum, _reason), _clientAddress);
                }

                _membership.dispose();
                signalListeners();

                return;
            }

            case COLLECT : {
                _value = _clientPost.getValue();

                if (_seqNum == LogStorage.EMPTY_LOG) {
                    _seqNum = 0;
                } else {
                    ++_seqNum;
                }

                if (isLeader()) {
                    _logger.info("Skipping collect phase - we're leader already");
                    _stage = BEGIN;
                    process();
                } else {
                    collect();
                    _stage = BEGIN;
                }

                break;
            }

            case BEGIN : {
                byte[] myValue = _value;

                // If we're not currently the leader, we'll have issued a collect and must process the responses
                //
                if (! isLeader()) {
                    // Process _messages to assess what we do next - might be to launch a new round or to give up
                    //
                    long myMaxRound = 0;
                    Iterator<PaxosMessage> myMessages = _messages.iterator();

                    while (myMessages.hasNext()) {
                        PaxosMessage myMessage = myMessages.next();

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

                amLeader();
                _value = myValue;

                begin();
                _stage = SUCCESS;

                break;
            }

            case SUCCESS : {

                /*
                 * Old round message, causes start at collect or quit.
                 * If Accept messages total more than majority we're happy, send Success wait for all acks
                 * or redo collect
                 */
                int myAcceptCount = 0;

                Iterator<PaxosMessage> myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

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
                    _stage = EXIT;
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    begin();
                    _stage = SUCCESS;
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _stage);
        }
    }

    /**
     * @todo We may get oldround after we've sent begin and got some accepts, in such a case we need collect to know that we
     * were interrupted and the client value has already been submitted and thus doesn't need to be re-run post recovery. Note that any
     * competing leader will first do a collect to increment the round number, it will not yet have proposed a value for a sequence number.
     * Thus if we've received an accept from some node, it has yet to see the new leader and when it does see the leader will return the value
     * we've proposed together with our round number.  Further if we've reached acceptance with our round number, ultimately our value will
     * be used as recovery dictates the value from the highest previous round for a sequence number will be used by the leader.  We have one issue
     * where some acceptor/learner did accept but we didn't get the message and thus can't tell the client value was processed.  What to do?
     * 
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        long myCompetingNodeId = myOldRound.getNodeId();

        notLeader();

        /*
         * Some other node is active, we should abort if they are the leader by virtue of a larger nodeId
         */
        if (myCompetingNodeId > _nodeId) {
            _logger.info("Superior leader is active, backing down: " + Long.toHexString(myCompetingNodeId) + ", " +
                    Long.toHexString(_nodeId));

            _stage = ABORT;
            _reason = Reasons.OTHER_LEADER;
            process();
            return;
        }

        updateRndNumber(myOldRound);

        /*
         * Some other leader is active but we are superior, restart negotiations with COLLECT, note we must mark ourselves as
         * not the leader (as has been done above) because having re-established leadership we must perform recovery and
         * then attempt to submit the client's value (if any) again.
         */
        _stage = COLLECT;
        process();
    }

    private void collect() {
        _messages.clear();

        PaxosMessage myMessage = new Collect(_seqNum, newRndNumber(), _nodeId);

        startInteraction();

        _logger.info("Leader sending collect: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void begin() {
        _messages.clear();

        PaxosMessage myMessage = new Begin(_seqNum, getRndNumber(), _nodeId, _value);

        startInteraction();

        _logger.info("Leader sending begin: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void success() {
        _messages.clear();

        PaxosMessage myMessage = new Success(_seqNum, _value);

        startInteraction();

        _logger.info("Leader sending success: " + Long.toHexString(_seqNum));

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
        _logger.info("Membership requested abort: " + Long.toHexString(_seqNum));

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
            process();
        }
    }

    private void expired() {
        _logger.info("Watchdog requested abort: " + Long.toHexString(_seqNum));

        synchronized(this) {
            _stage = ABORT;
            _reason = Reasons.VOTE_TIMEOUT;
            process();
        }
    }

    /**
     * @todo If we timeout and the client wants to retry, what to do with the sequence number?  It'll have been potentially incremented by the
     * previous BEGIN and thus we'll have left a gap.
     * @todo Modify collect to complete recovery leaving _seqNum at the last recovered sequence number because begin will need to increment it
     * 
     * @return BUSY if the state machine is not ready to process a request.
     */
    public int messageReceived(PaxosMessage aMessage, Address anAddress) {
        _logger.info("Leader received message: " + aMessage);

        if (aMessage.getType() == Operations.POST) {
            Post myPost = (Post) aMessage;

            synchronized(this) {
                if ((_stage != ABORT) && (_stage != EXIT)) {
                    return BUSY;
                }

                _clientPost = myPost;
                _clientAddress = anAddress;

                _logger.info("Initialising leader: " + Long.toHexString(_seqNum));

                // Collect will decide if it can skip straight to a begin
                //
                _stage = COLLECT;

                _membership = _detector.getMembers(this);

                _logger.info("Got membership for leader: " + Long.toHexString(_seqNum) + ", (" + _membership.getSize() + ")");

                process();
            }
        } else {
            synchronized(this) {
                if (aMessage.getSeqNum() == _seqNum) {
                    _messages.add(aMessage);
                    _membership.receivedResponse(anAddress);
                } else {
                    _logger.warn("Out of date message received: " + aMessage.getSeqNum() + " (" + Long.toHexString(_seqNum) + ")");
                }
            }
        }

        _logger.info("Leader processed message: " + aMessage);

        return CONTINUE;
    }

    private class Alarm extends TimerTask {
        public void run() {
            expired();
        }
    }

    private void signalListeners() {
        List myListeners;

        synchronized(_listeners) {
            myListeners = new ArrayList(_listeners);
        }

        Iterator<LeaderListener> myTargets = myListeners.iterator();

        while (myTargets.hasNext()) {
            myTargets.next().ready();
        }
    }
}
