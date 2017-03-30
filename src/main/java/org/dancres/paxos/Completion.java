package org.dancres.paxos;

/**
 * Asynchronous callback interface
 *
 * @param <T>
 */
public interface Completion<T> {
    /**
     * @param aValue for the completion
     */
    void complete(T aValue);
}
