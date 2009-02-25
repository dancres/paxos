package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.faildet.Membership;
import org.dancres.paxos.impl.faildet.MembershipListener;
import org.dancres.paxos.impl.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

class LeaderImpl implements MembershipListener {
    private static final int COLLECT = 0;
    private static final int BEGIN = 1;
    private static final int SUCCESS = 2;
    private static final int EXIT = 3;
    private static final int ABORT = 4;

    private long _seqNum;
    private byte[] _value;

    // Broadcast channel for all acceptor/learners
    //
    private Channel _channel;
    private Channel _clientChannel;

    private long _rndNumber = 0;

    private ProposerState _state;
    private Membership _membership;

    private int _stage = COLLECT;

    private List _messages = new ArrayList();

    private Logger _logger = LoggerFactory.getLogger(LeaderImpl.class);

    LeaderImpl(long aSeqNum, ProposerState aProposerState, Channel aBroadcastChannel, Channel aClientChannel) {
        _seqNum = aSeqNum;
        _state = aProposerState;
        _channel = aBroadcastChannel;
        _clientChannel = aClientChannel;
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
                    // Send failure message.....
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
        _logger.info("Leader dispatching collect: " + _seqNum);

        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Collect(_seqNum, _rndNumber, _state.getNodeId()),
                _state.getAddress().getPort());

        _membership.startInteraction();

        _logger.info("Leader sending collect: " + _seqNum);

        _channel.write(myMessage);
    }

    private void begin() {
        _logger.info("Leader dispatching begin: " + _seqNum);

        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Begin(_seqNum, _rndNumber, _state.getNodeId(), _value),
                _state.getAddress().getPort());

        _membership.startInteraction();

        _logger.info("Leader sending begin: " + _seqNum);

        _channel.write(myMessage);
    }

    private void success() {
        _logger.info("Leader dispatching success: " + _seqNum);

        _messages.clear();

        PaxosMessage myMessage = new ProposerHeader(new Success(_seqNum, _value), _state.getAddress().getPort());

        _membership.startInteraction();

        _logger.info("Leader sending success: " + _seqNum);

        _channel.write(myMessage);
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        synchronized(this) {
            _stage = ABORT;
            process();
        }
    }

    public void allReceived() {
        synchronized(this) {
            _stage++;
            process();
        }
    }

    /**
     * @todo Failure detector my not catch a blip in partition which would cause a single message to be lost, add a
     * timeout to cope with loss or do packet resends
     */
    void messageReceived(PaxosMessage aMessage) {
        _logger.info("Leader received message: " + aMessage);

        if (aMessage.getType() == Operations.POST) {
            _logger.info("Initialising leader: " + _seqNum);

            // Send a collect message
            _membership = _state.getFailureDetector().getMembers(this);

            _value = ((Post) aMessage).getValue();

            _logger.info("Got membership for leader: " + _seqNum);

            synchronized(this) {
                process();
            }
        } else {
            synchronized(this) {
                _messages.add(aMessage);
                _membership.receivedResponse();
            }
        }

        _logger.info("Leader processed message: " + aMessage);
    }
}
