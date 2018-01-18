package org.dancres.paxos.impl;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.bus.Messages;

import java.util.*;

class ProposalAllocator implements Messages.Subscriber<Constants.EVENTS> {

    private final int _maxInflight;

    private long _nextRnd;
    private long _nextSeq;
    private boolean _amLeader;
    private final SortedSet<Long> _recycling = new TreeSet<>();
    private final Set<Long> _inflight = new HashSet<>();
    private final Messages.Subscription<Constants.EVENTS> _bus;

    ProposalAllocator(Messages<Constants.EVENTS> aBus) {
        this(aBus, Constants.DEFAULT_MAX_INFLIGHT);
    }

    ProposalAllocator(Messages<Constants.EVENTS> aBus, int aMaxInflight) {
        _maxInflight = aMaxInflight;
        _amLeader = false;
        _bus = aBus.subscribe("ProposalAllocator", this);
    }

    ProposalAllocator resumeAt(long aSeqNum, long aRndNum) {
        _nextSeq = aSeqNum;
        _nextRnd = aRndNum + 1;

        return this;
    }

    boolean amLeader() {
        synchronized (_inflight) {
            return _amLeader;
        }
    }

    private static class NextInstance implements Instance {
        final long _rndNum;
        final long _seqNum;
        final Leader.State _state;

        NextInstance(Instance.State aState, long aSeqNum, long aRndNum) {
            _state = aState;
            _seqNum = aSeqNum;
            _rndNum = aRndNum;
        }

        public State getState() {
            return _state;
        }

        public long getRound() {
            return _rndNum;
        }

        public long getSeqNum() {
            return _seqNum;
        }

        @Override
        public Deque<VoteOutcome> getOutcomes() {
            throw new UnsupportedOperationException();
        }
    }

    public void msg(Messages.Message<Constants.EVENTS> aMessage) {
    }

    void conclusion(Instance anInstance, VoteOutcome anOutcome) {
        synchronized (_inflight) {
            // Is this instance invalidated due to other happenings?
            //
            if (! _inflight.remove(anInstance.getSeqNum()))
                return;

            switch (anOutcome.getResult()) {
                case VoteOutcome.Reason.VALUE : {
                    _amLeader = true;

                    break;
                }

                case VoteOutcome.Reason.OTHER_LEADER : {
                    _amLeader = false;
                    _nextRnd = anOutcome.getRndNumber() + 1;

                    Iterator<Long> myInstances = _inflight.iterator();

                    while (myInstances.hasNext())
                        if (myInstances.next() < anInstance.getSeqNum())
                            myInstances.remove();

                    if (_nextSeq < anOutcome.getSeqNum())
                        _nextSeq = anOutcome.getSeqNum();

                    while ((_recycling.size() > 0) && (_recycling.first() <= anOutcome.getSeqNum()))
                        _recycling.remove(_recycling.first());

                    break;
                }

                default : {
                    _amLeader = false;
                    _recycling.add(anInstance.getSeqNum());

                    break;
                }
            }

            if (_inflight.size() == 0)
                _bus.send(Constants.EVENTS.PROP_ALLOC_ALL_CONCLUDED, null);
        }
    }

    Instance nextInstance(long aPause) {
        long myExpiry = (aPause == 0) ? Long.MAX_VALUE : System.currentTimeMillis() + aPause;

        synchronized (_inflight) {
            if (! _amLeader) {
                while (_inflight.size() > 0) {
                    long myPause = myExpiry - System.currentTimeMillis();

                    if (myPause < 1)
                        return null;

                    try {
                        _inflight.wait(myPause);
                    } catch (InterruptedException anIE) {
                    }

                }

                _bus.send(Constants.EVENTS.PROP_ALLOC_INFLIGHT, null);

                return new NextInstance((_amLeader) ? Leader.State.BEGIN : Leader.State.COLLECT,
                        chooseNext(), _nextRnd);
            } else {
                while (_inflight.size() >= _maxInflight) {
                    long myPause = myExpiry - System.currentTimeMillis();

                    if (myPause < 1)
                        return null;

                    try {
                        _inflight.wait(myPause);
                    } catch (InterruptedException anIE) {
                    }

                }

                NextInstance myNext = new NextInstance((_amLeader) ? Leader.State.BEGIN : Leader.State.COLLECT,
                        chooseNext(), _nextRnd);

                if (_inflight.size() == 1)
                    _bus.send(Constants.EVENTS.PROP_ALLOC_INFLIGHT, null);

                return myNext;
            }
        }
    }

    private long chooseNext() {
        long myNext;

        if (_recycling.size() == 0) {
            myNext = ++_nextSeq;
        } else {
            myNext = _recycling.first();
            _recycling.remove(myNext);
        }

        _inflight.add(myNext);

        return myNext;
    }
}
