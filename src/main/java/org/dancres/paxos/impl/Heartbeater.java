package org.dancres.paxos.impl;

public interface Heartbeater {
    public void halt();	
    public void join() throws InterruptedException;
    public void start();
}