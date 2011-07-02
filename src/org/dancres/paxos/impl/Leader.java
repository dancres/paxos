package org.dancres.paxos.impl;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.Event;
import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the leader state machine.
 *
 * @todo State recovery - basic model will be to resolve memory state from the last checkpoint and then replay the log
 * to bring that memory state up-to-date (possibly by re-announcing completions via the AcceptorLearner).  Once that
 * is done, we could become the leader.
 * @todo If some other leader failed, we need to resolve any previous slots outstanding. It's possible the old leader
 * secured a slot and declared a value but didn't report back to the client. In that case, the client would see it's
 * leader fail to respond and have to either submit an operation to determine whether it's previous attempt succeeded
 * or submit an idempotent operation that could be retried safely multiple times. Other cases include the leader
 * reserving a slot but not filling in a value in which case we need to fill it with a null value (which AcceptorLearner
 * would need to drop silently rather than deliver it to a listener). It's also possible the leader proposed a value
 * but didn't reach a majority due to network issues and then died. In such a case, the new leader will have to settle
 * that round with the partially agreed value.  It's likely that will be done with the originating client absent so
 * as per the leader failed to report case, the client will need to retry with the appropriate protocol.
 * @todo Add a test for validating multiple sequence number recovery.
 * @todo Add a test for validating retries on dropped packets in later leader states.
 *
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    private static final long GRACE_PERIOD = 1000;

    private static final long MAX_TRIES = 3;

    /*
     * Internal states
     */

    /**
     * Leader reaches this state after SUBMITTED. If already leader, a transition to BEGIN will be immediate.
     * If not leader and recovery is not active, move to state RECOVER otherwise leader is in recovery and must now
     * settle low to high watermark (as set by RECOVER) before processing any submitted value by repeatedly executing
     * full instances of paxos (including a COLLECT to recover any previous value that was proposed).
     */
    private static final int COLLECT = 0;

    /**
     * Attempt to reserve a slot in the sequence of operations. Transition to SUCCESS after emitting begin to see
     * if the slot was granted.
     */
    private static final int BEGIN = 1;

    /**
     * Leader has sent a BEGIN and now determines if it has secured the slot associated with the sequence number.
     * If the slot was secured, a value will be sent to all members of the current instance after which there will
     * be a transition to COMMITTED.
     */
    private static final int SUCCESS = 2;

    /**
     * A paxos instance was completed successfully, clean up is all that remains.
     */
    private static final int EXIT = 3;

    /**
     * A paxos isntance failed for some reason (which will be found in </code>_completion</code>).
     */
    private static final int ABORT = 4;

    /**
     * Leader has been given a value and should attempt to complete a paxos instance.
     */
    private static final int SUBMITTED = 7;

    private final Timer _watchdog = new Timer("Leader timers");
    private final FailureDetector _detector;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = AcceptorLearner.UNKNOWN_SEQ;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without
     * risking breaking the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become
     * noisy when it should be silent). This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

    private long _tries = 0;

    /**
     * This alarm is used to limit the amount of time the leader will wait for responses from all apparently live
     * members in a round of communication.
     */
    private TimerTask _interactionAlarm;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;

    /**
     * Tracks membership for an entire paxos instance.
     */
    private Membership _membership;

    private int _state = EXIT;

    /**
     * In cases of ABORT, indicates the reason
     */
    private Event _event;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    private List<ConsolidatedValue> _queue = new LinkedList<ConsolidatedValue>();

    public Leader(FailureDetector aDetector, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _detector = aDetector;
        _transport = aTransport;
        _al = anAcceptorLearner;
    }

    public void shutdown() {
    	_watchdog.cancel();
    }

    private long calculateLeaderRefresh() {
        long myExpiry = _al.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 20 / 100);
    }

    private long calculateInteractionTimeout() {
        return GRACE_PERIOD;
    }

    public long getCurrentRound() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    public boolean isReady() {
        synchronized(this) {
            return (_state == EXIT) || (_state == ABORT);
        }
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state
     * we expect ACCEPT or OLDROUND
     *
     * @todo Increment round number via heartbeats every so often - see note below about jittering collects.
     */
    private void process() {
        switch(_state) {
            case ABORT : {
            	assert (_queue.size() != 0);

                _logger.info(this + ": ABORT " + _event, new RuntimeException());

                _messages.clear();

                if (_membership != null)
                    _membership.dispose();

                /*
                 * We must fail all queued operations - use the same completion code...
                 */
                while (_queue.size() > 0) {
                    _al.signal(new Event(_event.getResult(), _event.getSeqNum(),
                            _queue.remove(0)));
                }

                return;
            }

            case EXIT : {
            	assert (_queue.size() != 0);

            	_logger.info(this + ": EXIT " + _event);

                _messages.clear();

                // Remove the just processed item
                //                
                _queue.remove(0);

                if (_membership != null)
                    _membership.dispose();

                if (_queue.size() > 0) {
                    _logger.info(this + ": processing op from queue: " + _queue.get(0));

                    _state = SUBMITTED;
                    process();

                } else {

                	// If we got here, we're leader, setup for a heartbeat if there's no other activity
                	//
                    _heartbeatAlarm = new TimerTask() {
                        public void run() {
                            _logger.info(this + ": sending heartbeat: " + System.currentTimeMillis());

                            submit(AcceptorLearner.HEARTBEAT);
                        }
                    };

                    _watchdog.schedule(_heartbeatAlarm, calculateLeaderRefresh());
                }

                return;
            }

            case SUBMITTED : {
            	assert (_queue.size() != 0);

                _tries = 0;
                _membership = _detector.getMembers(this);

                _logger.info(this + ": got membership: (" +
                        _membership.getSize() + ")");

                // Collect will decide if it can skip straight to a begin
                //
                _state = COLLECT;
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
            	assert (_queue.size() != 0);

            	// We've got activity, cancel heartbeat until we're done. Note this may be a heartbeat, that's okay.
            	//
            	if (_heartbeatAlarm != null) {
            		_heartbeatAlarm.cancel();
                    _watchdog.purge();
                }

            	Collect myLastCollect = _al.getLastCollect();

            	// Collect is INITIAL means no leader known so try to become leader
            	//
            	if (myLastCollect.isInitial()) {
            		_logger.info(this + ": collect is initial");

                    _rndNumber = myLastCollect.getRndNumber() + 1;
            	} else {
            		InetSocketAddress myOtherLeader = myLastCollect.getNodeId();
            		boolean isUs = myOtherLeader.equals(_transport.getLocalAddress());

            		/*
            		 * If the leader is us we needn't update our round number and we can proceed, otherwise
            		 * we ascertain liveness of last known leader and proceed if it appears dead.
            		 *
            		 * This potential leader and its associated AL may be unaware of the active leader or indeed
            		 * it may appear dead. Under such circumstances this potential leader will attempt to
            		 * become anointed but could fail on an existing leader lease which would lead to a vote
            		 * timeout. The client would thus re-submit or similar rather than swap to the active leader node.
            		 * This is okay as eventually this potential leader will decide the active leader is alive and
            		 * immediately reject the client with an other leader message resulting in the necessary re-routing.
            		 *
            		 * The above behaviour has another benefit which is a client can connect to any member of the
            		 * paxos co-operative and be re-directed to the leader node. This means clients don't have to
            		 * perform any specialised behaviours for selecting a leader.
            		 */
            		if (! isUs) {

            			_logger.info(this + ": leader is not us");

            			if (_detector.isLive(myOtherLeader)) {
                            _logger.info(this + ": other leader is alive");

            				error(Event.Reason.OTHER_LEADER, myOtherLeader);
            				return;
            			} else {
            				_logger.info(this + ": other leader not alive");

                            /*
                             * If we're going for leadership, make sure we have a credible round number. If this
                             * round number isn't good enough, we'll find out via OldRound messages and update
                             * the round number accordingly.
                             */
                            if (_rndNumber <= myLastCollect.getRndNumber())
                                _rndNumber = myLastCollect.getRndNumber() + 1;
            			}
            		}
            	}

            	// Possibility we're starting from scratch
            	//
            	if (_seqNum == AcceptorLearner.UNKNOWN_SEQ)
            		_seqNum = 0;
            	else
            		_seqNum = _seqNum + 1;

            	_state = BEGIN;
                emit(new Collect(_seqNum, _rndNumber, _transport.getLocalAddress()));

            	break;
            }

            case BEGIN : {
            	assert (_queue.size() != 0);

            	long myMaxProposal = -1;
            	ConsolidatedValue myValue = null;

                for(PaxosMessage m : _messages) {
                    Last myLast = (Last) m;

                    if (!myLast.getConsolidatedValue().equals(LogStorage.NO_VALUE)) {
                        if (myLast.getRndNumber() > myMaxProposal) {
                            myValue = myLast.getConsolidatedValue();
                            myMaxProposal = myLast.getRndNumber();
                        }
                    }
                }

                /*
                 * If we have a value from a LAST message and it's not the same as the one we want to propose,
                 * we've hit an outstanding paxos instance and must now drive it to completion. Put it at the head of 
                 * the queue ready for the BEGIN. Note we must compare the consolidated value we want to propose
                 * as the one in the LAST message will be a consolidated value.
                 */
                if ((myValue != null) && (! myValue.equals(_queue.get(0))))
                	_queue.add(myValue);

                _state = SUCCESS;
                emit(new Begin(_seqNum, _rndNumber, _queue.get(0), _transport.getLocalAddress()));

                break;
            }

            case SUCCESS : {
            	assert (_queue.size() != 0);

                if (_messages.size() >= _detector.getMajority()) {
                    // Send success
                    //
                    emit(new Success(_seqNum, _rndNumber, _transport.getLocalAddress()));
                    cancelInteraction();

                    successful(Event.Reason.DECISION, null);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emit(new Begin(_seqNum, _rndNumber, _queue.get(0), _transport.getLocalAddress()));
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _state);
        }
    }

    private boolean canRetry() {
        return (_state == SUCCESS);
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        InetSocketAddress myCompetingNodeId = myOldRound.getLeaderNodeId();

        /*
         * If we're getting an OldRound, the other leader's lease has expired. We may be about to become leader
         * but if we're proposing against an already settled sequence number we need to get up to date. If we were
         * about to become leader but our sequence number is out of date, the response we'll get from an AL is an
         * OldRound where the round number is less than ours but the sequence number is greater. Note that updating
         * our _seqNum will drive our own AL to recover missing state should that be necessary.
         */
        if (myOldRound.getLastRound() < _rndNumber) {
            if (myOldRound.getSeqNum() > _seqNum) {
        	    _logger.info(this + ": This leader is out of date: " + _seqNum + " < " + myOldRound.getSeqNum());

                _seqNum = myOldRound.getSeqNum();

                _state = COLLECT;
                process();
            }
        } else {
        	_logger.info(this + ": Another leader is active, backing down: " + myCompetingNodeId + " (" +
                myOldRound.getLastRound() + ", " + _rndNumber + ")");

            _rndNumber = myOldRound.getLastRound() + 1;
            error(Event.Reason.OTHER_LEADER, myCompetingNodeId);
        }
    }

    private void successful(int aReason, Object aContext) {
        _state = EXIT;
        _event = new Event(aReason, _seqNum, _queue.get(0), aContext);

        process();
    }

    private void error(int aReason, Object aContext) {
        _state = ABORT;
        _event = new Event(aReason, _seqNum, _queue.get(0), aContext);

        process();
    }

    private void emit(PaxosMessage aMessage) {
        _messages.clear();

        if (startInteraction()) {
            _logger.info(this + ": sending: " + aMessage);

            _transport.send(aMessage, _transport.getBroadcastAddress());
        }
    }

    private boolean startInteraction() {
        assert _interactionAlarm == null;

        _interactionAlarm = new TimerTask() {
            public void run() {
                expired();
            }
        };

        _watchdog.schedule(_interactionAlarm, calculateInteractionTimeout());

        return _membership.startInteraction();
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        _logger.info(this + ": Membership requested abort");

        cancelInteraction();

        synchronized(this) {
            error(Event.Reason.BAD_MEMBERSHIP, null);
        }
    }

    public void allReceived() {
        cancelInteraction();

        synchronized(this) {
            _tries = 0;
            process();
        }
    }

    private void cancelInteraction() {
        assert _interactionAlarm != null;

        _interactionAlarm.cancel();
        _watchdog.purge();
        _interactionAlarm = null;
    }

    private void expired() {
        // _interactionAlarm will be cancelled automatically by watchdog
        //
        _logger.info(this + ": Watchdog requested abort: ");

        synchronized(this) {
            if (canRetry()) {
                ++_tries;

                if (_tries < MAX_TRIES) {
                    process();
                    return;
                }
            }

            error(Event.Reason.VOTE_TIMEOUT, null);
        }
    }

    public void submit(byte[] aValue, byte[] aHandback) {
        submit(new ConsolidatedValue(aValue, aHandback));
    }

    /**
     * Request a vote on a value.
     *
     * @param aValue is the value to attempt to agree upon
     */
    private void submit(ConsolidatedValue aValue) {
        synchronized (this) {
        	_queue.add(aValue);

            if (! isReady()) {
                _logger.info(this + ": Queued operation (already active): " + aValue);
            } else {
                _logger.info(this + ": Queued operation (initialising leader)");

                _state = SUBMITTED;

                process();
            }
        }
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

		_logger.info(this + " received message: " + aMessage);

        synchronized (this) {
            if ((aMessage.getSeqNum() == _seqNum) && MessageValidator.getValidator(_state).acceptable(aMessage)) {
                if (MessageValidator.getValidator(_state).fail(aMessage)) {

                    // Can only be an oldRound right now...
                    //
                    oldRound(aMessage);
                } else {
                    _messages.add(aMessage);
                    _membership.receivedResponse(aMessage.getNodeId());
                }
            } else {
                _logger.warn(this + ": Unexpected message received: " + aMessage);
            }
        }
    }

    public String toString() {
        int myState;

        synchronized(this) {
            myState = _state;
        }

    	return "Leader: " + _transport.getLocalAddress() +
    		": (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")" + " in state: " + myState +
                " tries: " + _tries + "/" + MAX_TRIES;
    }

    private static abstract class MessageValidator {
        private static final MessageValidator _beginValidator =
                new MessageValidator(new Class[]{OldRound.class, Last.class}, new Class[]{OldRound.class}) {};

        private static final MessageValidator _successValidator =
                new MessageValidator(new Class[]{OldRound.class, Accept.class}, new Class[]{OldRound.class}){};

        private static final MessageValidator _nullValidator =
                new MessageValidator(new Class[]{}, new Class[]{}) {};

        static MessageValidator getValidator(int aLeaderState) {
            switch(aLeaderState) {
                case BEGIN : return _beginValidator;

                case SUCCESS : return _successValidator;

                default : throw new RuntimeException("No validator for state: " + aLeaderState);
            }
        }

        private Class[] _acceptable;
        private Class[] _fail;

        private MessageValidator(Class[] acceptable, Class[] fail) {
            _acceptable = acceptable;
            _fail = fail;
        }

        boolean acceptable(PaxosMessage aMessage) {
            for (Class c: _acceptable) {
                if (aMessage.getClass().equals(c))
                    return true;
            }

            return false;
        }

        boolean fail(PaxosMessage aMessage) {
            for (Class c: _fail) {
                if (aMessage.getClass().equals(c))
                    return true;
            }

            return false;
        }
    }
}
