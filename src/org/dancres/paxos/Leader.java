package org.dancres.paxos;

import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
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
 * 
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    /*
     * Used to compute the timeout period for watchdog tasks.  In order to behave sanely we want the failure 
     * detector to be given the best possible chance of detecting problems with the members.  Thus the timeout for the
     * watchdog is computed as the unresponsiveness threshold of the failure detector plus a grace period.
     */
    private static final long FAILURE_DETECTOR_GRACE_PERIOD = 2000;

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
     * Leader has completed the necessary steps to secure an entry in storage for a particular sequence number
     * and is now processing ACKs to ensure enough nodes have seen the committed value.
     */
    private static final int COMMITTED = 6;

    /**
     * Leader has been given a value and should attempt to complete a paxos instance.
     */
    private static final int SUBMITTED = 7;

    private final Timer _watchdog = new Timer("Leader timers");
    private final FailureDetector _detector;
    private final Transport _transport;
    private final AcceptorLearner _al;
    private final MessageValidator _msgValidator = new MessageValidator();

    private long _seqNum = AcceptorLearner.UNKNOWN_SEQ;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without 
     * risking breaking the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become
     * noisy when it should be silent). This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

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
        return _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;    	
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
                
                // Remove the just processed item
                //
                _queue.remove(0);
                
                if (_membership != null)
                    _membership.dispose();

                _al.signal(_event);

                /*
                 * If there are any queued operations we must fail those also - use the same completion code...
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
            	if (_heartbeatAlarm != null)
            		_heartbeatAlarm.cancel();
            	
            	Collect myLastCollect = _al.getLastCollect();

            	// Collect is INITIAL means no leader known so try to become leader
            	//
            	if (myLastCollect.isInitial()) {
            		_logger.info(this + ": collect is initial");

                    _rndNumber = myLastCollect.getRndNumber() + 1;
            	} else {
            		NodeId myOtherLeader = NodeId.from(myLastCollect.getNodeId());
            		boolean isUs = myOtherLeader.equals(_transport.getLocalNodeId());
            		
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
            	emitCollect();
            	
            	break;
            }

            case BEGIN : {
            	assert (_queue.size() != 0);
            	
            	long myMaxProposal = -1;
            	ConsolidatedValue myValue = null;
            	
                Iterator<PaxosMessage> myMessages = _messages.iterator();

                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        /*
                         * An OldRound here indicates some other leader is present, give up.
                         */
                        oldRound(myMessage);
                        return;
                    } else {
                        Last myLast = (Last) myMessage;
                        
                        if (! myLast.getConsolidatedValue().equals(LogStorage.NO_VALUE)) {
                        	if (myLast.getRndNumber() > myMaxProposal)
                        		myValue = myLast.getConsolidatedValue();
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
                emitBegin();
                
                break;
            }

            case SUCCESS : {
            	assert (_queue.size() != 0);
            	
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
                    _state = COMMITTED;
                    emitSuccess();
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    _state = SUCCESS;
                    emitBegin();
                }

                break;
            }

            case COMMITTED : {
            	assert (_queue.size() != 0);
            	
                /*
                 * If ACK messages total more than majority we're happy otherwise try again.
                 */
                if (_messages.size() >= _membership.getMajority()) {
                    successful(Event.Reason.DECISION, null);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    _state = COMMITTED;
                    emitSuccess();
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _state);
        }
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        NodeId myCompetingNodeId = NodeId.from(myOldRound.getLeaderNodeId());

        /*
         * If we're getting an OldRound, the other leader's lease has expired. We may be about to become leader
         * but if we're proposing against an already settled sequence number we need to get up to date. If we were
         * about to become leader but our sequence number is out of date, the response we'll get from an AL is an
         * OldRound where the round number is less than ours but the sequence number is greater.
         */
        if (myOldRound.getLastRound() < _rndNumber) {
            if (myOldRound.getSeqNum() > _seqNum) {
                _seqNum = myOldRound.getSeqNum();

        	    _logger.info(this + ": This leader is out of date: " + _seqNum + " < " + myOldRound.getSeqNum());

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

    private void emitCollect() {
        emit(new Collect(_seqNum, _rndNumber, _transport.getLocalNodeId().asLong()));
    }

    private void emitBegin() {
        emit(new Begin(_seqNum, _rndNumber, _queue.get(0),
        		_transport.getLocalNodeId().asLong()));
    }

    private void emitSuccess() {
        emit(new Success(_seqNum, _rndNumber,
        		_queue.get(0), _transport.getLocalNodeId().asLong()));
    }

    private void emit(PaxosMessage aMessage) {
        _messages.clear();

        if (!startInteraction())
        	return;

        _logger.info(this + ": sending: " + aMessage);

        _transport.send(aMessage, NodeId.BROADCAST);
    }

    private boolean startInteraction() {
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

        _interactionAlarm.cancel();
        
        synchronized(this) {
            error(Event.Reason.BAD_MEMBERSHIP, null);
        }
    }

    private void expired() {
        _logger.info(this + ": Watchdog requested abort: ");

        synchronized(this) {
            // If there are enough messages we might get a majority continue
            //
            if (_messages.size() >= _membership.getMajority())
                process();
            else {
                error(Event.Reason.VOTE_TIMEOUT, null);
            }
        }
    }

    public void allReceived() {
        _interactionAlarm.cancel();

        synchronized(this) {
            process();
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
                return;
            }

            _logger.info(this + ": Queued operation (initialising leader)");

            _state = SUBMITTED;

            process();
        }
    }

    /**
     * Used to process all core paxos protocol messages.
     *
     * @param aMessage is a message from some acceptor/learner
     */
    public void messageReceived(PaxosMessage aMessage) {
    	if (aMessage.getClassification() == PaxosMessage.CLIENT) {
            throw new IllegalArgumentException("Not going to handle that CLIENT message for you");
    	}
    	
		_logger.info(this + " received message: " + aMessage);
        
        synchronized (this) {
            if ((aMessage.getSeqNum() == _seqNum) && _msgValidator.acceptable(aMessage, _state)) {
                _messages.add(aMessage);
                _membership.receivedResponse(NodeId.from(aMessage.getNodeId()));
            } else {
                _logger.warn(this + ": Unexpected message received: " + aMessage.getSeqNum());
            }
        }
    }
    
    public String toString() {
        int myState;

        synchronized(this) {
            myState = _state;
        }

    	return "Leader: " + _transport.getLocalNodeId() +
    		": (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")" + " in state: " + myState;
    }

    private static class MessageValidator {
        private static final Map<Integer, Class[]> _acceptableResponses = new HashMap<Integer, Class[]>();

        static {
            _acceptableResponses.put(new Integer(COMMITTED), new Class[]{Ack.class});
            _acceptableResponses.put(new Integer(SUCCESS), new Class[]{OldRound.class, Accept.class});
            _acceptableResponses.put(new Integer(BEGIN), new Class[]{OldRound.class, Last.class});
        }

        boolean acceptable(PaxosMessage aMessage, int aLeaderState) {
            Class[] myAcceptableTypes = _acceptableResponses.get(new Integer(aLeaderState));

            if (myAcceptableTypes == null)
                throw new RuntimeException("Not got a set of expected types for this state :(");

            for (int i = 0; i < myAcceptableTypes.length; i++) {
                if (aMessage.getClass().equals(myAcceptableTypes[i]))
                    return true;
            }

            return false;
        }
    }
}
