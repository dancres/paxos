package org.dancres.paxos.impl.faildet;

public interface Membership {
    public int getSize();
    public void startInteraction();
    public void receivedResponse();
    public void dispose();
    public int getMajority();
}
