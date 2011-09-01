package org.dancres.paxos.impl;

public interface Producer {
    public void produce(long aLogOffset) throws Exception;
}
