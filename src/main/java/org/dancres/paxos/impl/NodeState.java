package org.dancres.paxos.impl;

import java.util.concurrent.atomic.AtomicReference;

class NodeState {
    public enum State {INITIAL, ACTIVE, RECOVERING, OUT_OF_DATE, SHUTDOWN}

    private final AtomicReference<State> _state = new AtomicReference<>(State.INITIAL);

    void set(State aState) {
        _state.getAndSet(aState);
    }

    boolean testAndSet(State anExpected, State aNewState) {
        return _state.compareAndSet(anExpected, aNewState);
    }

    boolean test(State aState) {
        return _state.get().equals(aState);
    }
}
