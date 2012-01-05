package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.TimerTask;

/**
 * Implements the leader state machine for a specific instance of Paxos. Leader is fail fast in that the first time
 * it spots a problem, it will cease any further attempts to drive progress reporting the issue and leaving user-code
 * to re-submit a request. This applies equally to handling conflicting values (that might occur as the result of a
 * need to drive a previous instance to completion as the result of LAST responses).
 *
 * @todo Add a test for validating retries on dropped packets in later leader states.
 *
 * @author dan
 */
public class Leader implements MembershipListener {
    
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    private static final long GRACE_PERIOD = 1000;

    private static final long MAX_TRIES = 3;

    /**
     * Leader reaches COLLECT after SUBMITTED unless <code>LeaderFactory</code> overrides that to transition to
     * BEGIN (the multi-paxos optimisation).
     * 
     * In BEGIN we attempt to reserve a slot in the sequence of operations. Transition to SUCCESS after emitting begin 
     * to see if the slot was granted.
     * 
     * In SUCCESS, Leader has sent a BEGIN and now determines if it has secured the slot associated with the sequence 
     * number. If the slot was secured, a value will be sent to all members of the current instance after which there
     * will be a transition to COMMITTED.
     * 
     * In EXIT a paxos instance was completed successfully, clean up is all that remains.
     * 
     * In ABORT a paxos instance failed for some reason (which will be found in </code>_event</code>).
     * 
     * In SUBMITTED, Leader has been given a value and should attempt to complete a paxos instance.
     *
     * In SHUTDOWN, we do a little cleanup and halt, processing no messages etc.
     */
    public enum States {
    	INITIAL, SUBMITTED, COLLECT, BEGIN, SUCCESS, EXIT, ABORT, SHUTDOWN
    }
    

    private final Common _common;
    private final LeaderFactory _factory;

    private final long _seqNum;
    private final long _rndNumber;
    private long _tries = 0;

    private Proposal _prop;

    /**
     * This alarm is used to limit the amount of time the leader will wait for responses from all apparently live
     * members in a round of communication.
     */
    private TimerTask _interactionAlarm;

    /**
     * Tracks membership for an entire paxos instance.
     */
    private Membership _membership;

    private final States _startState;
    private States _currentState = States.INITIAL;

    /**
     * In cases of ABORT, indicates the reason
     */
    private VoteOutcome _event;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    public Leader(Common aCommon, LeaderFactory aFactory,
                  long aNextSeq, long aRndNumber) {
        _common = aCommon;
        _factory = aFactory;
        _seqNum = aNextSeq;
        _rndNumber = aRndNumber;
        _startState = States.COLLECT;
    }

    public Leader(Common aCommon, LeaderFactory aFactory,
                  long aNextSeq, long aRndNumber, States aStartState) {
        _common = aCommon;
        _factory = aFactory;
        _seqNum = aNextSeq;
        _rndNumber = aRndNumber;
        _startState = aStartState;
    }

    VoteOutcome getOutcome() {
        synchronized(this) {
            return _event;
        }
    }

    void shutdown() {
    	synchronized(this) {
            if (! isDone()) {
    		    _currentState = States.SHUTDOWN;
                _event = null;
                process();
            }
    	}
    }

    private long calculateInteractionTimeout() {
        return GRACE_PERIOD;
    }

    public long getRound() {
        return _rndNumber;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public States getState() {
        synchronized(this) {
            return _currentState;
        }
    }

    boolean isDone() {
        synchronized(this) {
            return ((_currentState.equals(States.EXIT)) || (_currentState.equals(States.ABORT)) ||
                    (_currentState.equals(States.SHUTDOWN)));
        }
    }

   private void cleanUp() {
       _messages.clear();

       if (_membership != null)
           _membership.dispose();       
   }
    
    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state
     * we expect ACCEPT or OLDROUND
     */
    private void process() {
        switch (_currentState) {
            case SHUTDOWN : {
                _logger.info(this + ": SHUTDOWN");
                
                cleanUp();

                _currentState = States.ABORT;

                return;
            }

            case ABORT : {
                _logger.info(this + ": ABORT " + _event);

                cleanUp();

                cancelInteraction();

                _common.signal(_event);

                _factory.dispose(this);
                
                return;
            }

            case EXIT : {
            	_logger.info(this + ": EXIT " + _event);

                cleanUp();

                _factory.dispose(this);

                return;
            }

            case SUBMITTED : {
                _tries = 0;
                _membership = _common.getFD().getMembers(this);

                _logger.debug(this + ": got membership: (" +
                        _membership.getSize() + ")");

                _currentState = _startState;
                process();

                break;
            }

            /*
             * It's possible an AL will have seen a success that no others saw such that a previous value is 
             * not fully committed. That's okay as a lagging leader will propose a new client value for that sequence
             * number and find that AL tells it about this value which will cause the leader to finish off that
             * round and any others after which it can propose the client value for a sequence number. Should that AL
             * die the record is lost and the client needs to re-propose the value.
             * 
             * Other AL's may have missed other values, that's also okay as they will separately deduce they have
             * missing instances to catch-up and recover that state from those around them.
             */
            case COLLECT : {
            	_currentState = States.BEGIN;
                emit(new Collect(_seqNum, _rndNumber, _common.getTransport().getLocalAddress()));

            	break;
            }

            case BEGIN : {
                Last myLast = null;
                
                for(PaxosMessage m : _messages) {
                    Last myNewLast = (Last) m;

                    if (!myNewLast.getConsolidatedValue().equals(Proposal.NO_VALUE)) {
                        if (myLast == null)
                            myLast = myNewLast;
                        else if (myNewLast.getRndNumber() > myLast.getRndNumber()) {
                            myLast = myNewLast;
                        }
                    }
                }

                /*
                 * If we have a value from a LAST message and it's not the same as the one we want to propose,
                 * we've hit an outstanding paxos instance and must now drive it to completion. Note we must
                 * compare the consolidated value we want to propose as the one in the LAST message will be a
                 * consolidated value.
                 */
                if ((myLast != null) && (! myLast.getConsolidatedValue().equals(_prop))) {
                    _common.signal(new VoteOutcome(VoteOutcome.Reason.OTHER_VALUE,
                            _seqNum, _rndNumber, _prop, myLast.getNodeId()));

                    _prop = myLast.getConsolidatedValue();
                }

                _currentState = States.SUCCESS;
                emit(new Begin(_seqNum, _rndNumber, _prop,
                        _common.getTransport().getLocalAddress()));

                break;
            }

            case SUCCESS : {
                if (_messages.size() >= _common.getFD().getMajority()) {
                    // Send success
                    //
                    emit(new Success(_seqNum, _rndNumber, _common.getTransport().getLocalAddress()));
                    cancelInteraction();
                    successful(VoteOutcome.Reason.DECISION);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emit(new Begin(_seqNum, _rndNumber, _prop,
                            _common.getTransport().getLocalAddress()));
                }

                break;
            }

            default : throw new Error("Invalid state: " + _currentState);
        }
    }

    private boolean canRetry() {
        return _currentState.equals(States.SUCCESS);
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        InetSocketAddress myCompetingNodeId = myOldRound.getLeaderNodeId();

        _logger.info(this + ": Another leader is active, backing down: " + myCompetingNodeId + " (" +
                Long.toHexString(myOldRound.getLastRound()) + ", " + Long.toHexString(_rndNumber) + ")");

        _currentState = States.ABORT;
        _event = new VoteOutcome(VoteOutcome.Reason.OTHER_LEADER, myOldRound.getSeqNum(),
                myOldRound.getLastRound(), _prop, myCompetingNodeId);

        process();
    }

    private void successful(int aReason) {
        _currentState = States.EXIT;
        _event = new VoteOutcome(aReason, _seqNum, _rndNumber, _prop,
                _common.getTransport().getLocalAddress());

        process();
    }

    private void error(int aReason) {
    	error(aReason, _common.getTransport().getLocalAddress());
    }
    
    private void error(int aReason, InetSocketAddress aLeader) {
        _currentState = States.ABORT;
        _event = new VoteOutcome(aReason, _seqNum, _rndNumber, _prop, aLeader);
        
        _logger.info("Leader encountered error: " + _event);

        process();
    }

    private void emit(PaxosMessage aMessage) {
        _messages.clear();

        if (startInteraction()) {
            _logger.info(this + ": tx: " + aMessage);

            _common.getTransport().send(aMessage, _common.getTransport().getBroadcastAddress());
        }
    }

    private boolean startInteraction() {
        assert _interactionAlarm == null;

        _interactionAlarm = new TimerTask() {
            public void run() {
                expired();
            }
        };

        _common.getWatchdog().schedule(_interactionAlarm, calculateInteractionTimeout());

        return _membership.startInteraction();
    }

    public void abort() {
        _logger.info(this + ": Membership requested abort");

        synchronized(this) {
            error(VoteOutcome.Reason.BAD_MEMBERSHIP);
        }
    }

    public void allReceived() {
        synchronized(this) {
            cancelInteraction();

            _tries = 0;
            process();
        }
    }

    private void cancelInteraction() {
        assert _interactionAlarm != null;

        _interactionAlarm.cancel();
        _common.getWatchdog().purge();
        _interactionAlarm = null;
    }

    private void expired() {
        _logger.info(this + ": Watchdog requested abort: ");

        synchronized(this) {
            if (canRetry()) {
                ++_tries;

                if (_tries < MAX_TRIES) {
                	cancelInteraction();
                    process();
                    return;
                }
            }

            error(VoteOutcome.Reason.VOTE_TIMEOUT);
        }
    }

    /**
     * Request a vote on a value.
     *
     * @param aValue is the value to attempt to agree upon
     */
    public void submit(Proposal aValue) {
        synchronized (this) {
            if (_currentState != States.INITIAL)
                throw new IllegalStateException("Submit already done, create another leader");

            _logger.info(this + ": Submitted operation (initialising leader)");

            _prop = aValue;

            _currentState = States.SUBMITTED;

            process();
        }
    }

    boolean isFail(PaxosMessage aMessage) {
        return (aMessage instanceof OldRound);
    }
    
    /**
     * Used to process all core paxos protocol messages.
     *
     * We optimise by counting messages and transitioning as soon as we have enough and detecting failure
     * immediately. But what if we miss an oldRound? If we miss an OldRound it can only be because a minority is seeing
     * another leader and when it runs into our majority, it will be forced to resync seqNum/learnedValues etc. In
     * essence if we've progressed through enough phases to get a majority commit we can go ahead and set the value as
     * any future leader wading in will pick up our value. NOTE: This optimisation requires the membership impl to
     * understand the concept of minimum acceptable majority.
     *
     * @param aMessage is a message from some acceptor/learner
     */
    public void messageReceived(PaxosMessage aMessage) {
        assert (aMessage.getClassification() != PaxosMessage.CLIENT): "Got a client message and shouldn't have done";

        synchronized (this) {
            switch (_currentState) {
                case ABORT :
                case EXIT :
                case SHUTDOWN : {
                    return;
                }
            }

            _logger.info(this + " rx: " + aMessage);

            if (aMessage instanceof LeaderSelection) {
                if (((LeaderSelection) aMessage).routeable(this)) {
                    if (isFail(aMessage)) {

                        // Can only be an oldRound right now...
                        //
                        oldRound(aMessage);
                    } else {
                        _messages.add(aMessage);
                        _membership.receivedResponse(aMessage.getNodeId());
                    }
                    return;
                }
            }

            _logger.warn(this + ": Unexpected message received: " + aMessage);
        }
    }

    public String toString() {
        States myState;

        synchronized(this) {
            myState = _currentState;
        }

    	return "Leader: " + _common.getTransport().getLocalAddress() +
    		": (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")" + " in state: " + myState +
                " tries: " + _tries + "/" + MAX_TRIES;
    }
}
