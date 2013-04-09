package org.dancres.paxos;

/**
 * Concrete generic implementation of a completion that allows a caller to block pending the callback.
 *
 * @param <T>
 */
public class CompletionImpl<T> implements Completion<T> {
    private T _result;

    public void complete(T anOutcome) {
        synchronized (this) {
            _result = anOutcome;
            notify();
        }
    }

    public T await() {
        synchronized (this) {
            while (_result == null) {
                try {
                    wait();
                } catch (InterruptedException anIE) {}
            }

            return _result;
        }
    }
}

