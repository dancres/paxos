package org.dancres.paxos.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.*;

public class NetworkUtils {
    private static Logger _logger = LoggerFactory.getLogger(NetworkUtils.class);

    private static NetworkInterface _workableInterface = null;
    private static InetAddress _workableAddress = null;

    /*
     * Iterate interfaces and look for one that's multicast capable and not a 127 or 169 based address
     */
    static {
        SortedSet<NetworkInterface> myWorkableInterfaces = 
        	new TreeSet<NetworkInterface>(new NetworkInterfaceComparator());

        try {
            Enumeration<NetworkInterface> myInterfaces = NetworkInterface.getNetworkInterfaces();

            while (myInterfaces.hasMoreElements()) {
                NetworkInterface myInterface = myInterfaces.nextElement();

                _logger.debug("Checking interface: " + myInterface.getDisplayName());

                if (! isMulticastCapable(myInterface))
                    continue;

                if ((! myInterface.getName().startsWith("en")) &&
                        (! myInterface.getName().startsWith("eth")))
                    continue;

                if (hasValidAddress(myInterface))
                    myWorkableInterfaces.add(myInterface);
            }

            Iterator<NetworkInterface> myPossibles = myWorkableInterfaces.iterator();
            while (myPossibles.hasNext()) {
                _logger.debug("Candidate Interface: " + myPossibles.next());
            }

            /*
             * We would prefer to use the index number to choose but in the absence of that we choose the
             * interface with the lowest index
             */
            _workableInterface = myWorkableInterfaces.first();
            _workableAddress = getValidAddress(_workableInterface);

            _logger.debug("Equates to address: " + _workableAddress);
        } catch (Exception anE) {
            throw new Error("Failed to find interface", anE);
        }
    }

    private static class NetworkInterfaceComparator implements Comparator<NetworkInterface> {

        public int compare(NetworkInterface anNi, NetworkInterface anotherNi) {
            String myAName = anNi.getName();
            String myBName = anotherNi.getName();

            return myAName.compareTo(myBName);
        }
    }

    private static InetAddress getValidAddress(NetworkInterface anIn) {
        Enumeration<InetAddress> myAddrs = anIn.getInetAddresses();

        while (myAddrs.hasMoreElements()) {
            InetAddress myAddr = myAddrs.nextElement();

            // If it's not IPv4, forget it
            if (myAddr.getAddress().length != 4)
                continue;

            boolean isReachable = false;

            if (myAddr.isLoopbackAddress())
                continue;

            try {
                isReachable = myAddr.isReachable(500);
            } catch (Exception anE) {
                _logger.debug("Not reachable: " + myAddr, anE);
                continue;
            }

            if (!isReachable)
                continue;

            // Found one address on this interface that makes sense
            //
            return myAddr;
        }

        return null;
    }

    private static boolean hasValidAddress(NetworkInterface anIn) {
        return (getValidAddress(anIn) != null);
    }

    private static boolean isMulticastCapable(NetworkInterface anIn) {
        try {
            InetAddress myMcast = InetAddress.getByName("224.0.1.85");

            MulticastSocket mySocket = new MulticastSocket(4159);

            mySocket.setNetworkInterface(anIn);

            mySocket.joinGroup(myMcast);

            String myMsg = "blahblah";

            DatagramPacket myPkt = new DatagramPacket(myMsg.getBytes(), myMsg.length(),
                             myMcast, 6789);

            mySocket.send(myPkt);

            mySocket.close();

            return true;
        } catch (Exception anE) {
            _logger.debug("No mcast: " + anIn, anE);
            return false;
        }
    }

    public static InetAddress getWorkableInterface() {
        return _workableAddress;
    }

    public static InetAddress getBroadcastAddress() {
        return _workableInterface.getInterfaceAddresses().get(0).getBroadcast();
    }

    public static int getAddressSize() {
        return 4;
    }
}
