package org.dancres.paxos;

public interface Completion<T> {
    /**
     * @param aValue for the completion
     */
    public void complete(T aValue);
}
