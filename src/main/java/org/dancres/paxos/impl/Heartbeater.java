package org.dancres.paxos.impl;

public interface Heartbeater {
    void halt();
    void join() throws InterruptedException;
    void start();
}