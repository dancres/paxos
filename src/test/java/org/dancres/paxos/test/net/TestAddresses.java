package org.dancres.paxos.test.net;

import org.dancres.paxos.impl.net.Utils;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TestAddresses {
    private static final AtomicInteger _portAllocator = new AtomicInteger(2048);

    public static InetSocketAddress next() {
        return new InetSocketAddress(Utils.getWorkableInterfaceAddress(), _portAllocator.getAndIncrement());
    }
}
