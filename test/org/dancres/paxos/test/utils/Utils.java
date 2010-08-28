package org.dancres.paxos.test.utils;

import org.dancres.paxos.impl.NetworkUtils;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {
    private static AtomicInteger _portAllocator = new AtomicInteger(2048);

    public static InetSocketAddress getTestAddress() {
        return new InetSocketAddress(NetworkUtils.getWorkableInterface(), _portAllocator.getAndIncrement());
    }
}
