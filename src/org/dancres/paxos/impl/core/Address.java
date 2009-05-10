package org.dancres.paxos.impl.core;

/**
 * Standard abstraction for the address of a member in the Paxos co-operative.
 *
 * @author dan
 */
public interface Address {
    public static final Address BROADCAST = new Address() {};
}
