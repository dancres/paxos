package org.dancres.paxos.messages;

public interface Claim extends PaxosMessage {
    long getRndNumber();
}
