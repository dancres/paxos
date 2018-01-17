package org.dancres.util;

import java.util.concurrent.*;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public abstract class AbstractFuture<T> implements Future<T> {
    private final Sync _sync = new Sync();

    public boolean cancel(boolean canInterrupt) {
        return _sync.cancel();
    }

    public boolean isCancelled() {
        return _sync.isCancelled();
    }

    public boolean isDone() {
        return _sync.isDone();
    }

    public T get() throws InterruptedException, ExecutionException {
        return _sync.get();
    }

    public T get(long aTimeout, TimeUnit aUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return _sync.get(aUnit.toNanos(aTimeout));
    }

    protected void set(T aT) {
        _sync.set(aT);
    }

    protected abstract void done();

    private final class Sync extends AbstractQueuedSynchronizer {
        private static final long serialVersionUID = -7828117401763700385L;

        private static final int DONE = 1;
        private static final int CANCELLED = 2;

        private T _result;
        private Throwable _exception;

        private boolean doneOrCancelled(int aState) {
            return (aState & (DONE | CANCELLED)) != 0;
        }

        protected int tryAcquireShared(int anArg) {
            return isDone() ? 1 : -1;
        }

        protected boolean tryReleaseShared(int anArg) {
            return true;
        }

        boolean isCancelled() {
            return getState() == CANCELLED;
        }

        boolean isDone() {
            return doneOrCancelled(getState());
        }

        T get() throws InterruptedException, ExecutionException {
            acquireSharedInterruptibly(0);

            if (getState() == CANCELLED)
                throw new CancellationException();

            if (_exception != null)
                throw new ExecutionException(_exception);

            return _result;
        }

        T get(long aTimeout) throws InterruptedException, ExecutionException, TimeoutException {
            if (!tryAcquireSharedNanos(0, aTimeout))
                throw new TimeoutException();

            if (getState() == CANCELLED)
                throw new CancellationException();

            if (_exception != null)
                throw new ExecutionException(_exception);

            return _result;
        }

        void set(T aT) {
            while(true) {
                int myState = getState();

                if (myState == DONE)
                    return;

                if (myState == CANCELLED) {
                    releaseShared(0);
                    return;
                }

                if (compareAndSetState(myState, DONE)) {
                    _result = aT;
                    releaseShared(0);
                    done();
                    return;
                }
            }
        }

        boolean cancel() {
            while(true)  {
                int s = getState();
                if (doneOrCancelled(s))
                    return false;
                if (compareAndSetState(s, CANCELLED))
                    break;
            }

            releaseShared(0);
            done();
            return true;
        }
    }
}
