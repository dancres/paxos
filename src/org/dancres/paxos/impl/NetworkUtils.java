package org.dancres.paxos.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mina.common.IoBuffer;

import java.net.*;
import java.util.*;

public class NetworkUtils {
    private static Logger _logger = LoggerFactory.getLogger(NetworkUtils.class);

    private static InetAddress _workableAddress = null;

    /*
     * Iterate interfaces and look for one that's multicast capable and not a 127 or 169 based address
     */
    static {
        SortedSet myWorkableInterfaces = new TreeSet(new NetworkInterfaceComparator());

        try {
            Enumeration myInterfaces = NetworkInterface.getNetworkInterfaces();

            while (myInterfaces.hasMoreElements()) {
                NetworkInterface myInterface = (NetworkInterface) myInterfaces.nextElement();

                _logger.debug("Checking interface: " + myInterface.getDisplayName());

                if (! isMulticastCapable(myInterface))
                    continue;

                if ((! myInterface.getName().startsWith("en")) &&
                        (! myInterface.getName().startsWith("eth")))
                    continue;

                if (hasValidAddress(myInterface))
                    myWorkableInterfaces.add(myInterface);
            }

            Iterator myPossibles = myWorkableInterfaces.iterator();
            while (myPossibles.hasNext()) {
                _logger.debug("Candidate Interface: " + myPossibles.next());
            }

            /*
             * We would prefer to use the index number to choose but in the absence of that we choose the
             * interface with the lowest index
             */
            NetworkInterface myLowest = (NetworkInterface) myWorkableInterfaces.first();
            _workableAddress = getValidAddress(myLowest);

            _logger.debug("Equates to address: " + _workableAddress);
        } catch (Exception anE) {
            throw new Error("Failed to find interface", anE);
        }
    }

    private static class NetworkInterfaceComparator implements Comparator {

        public int compare(Object anO, Object anotherO) {
            NetworkInterface myA = (NetworkInterface) anO;
            NetworkInterface myB = (NetworkInterface) anotherO;

            String myAName = myA.getName();
            String myBName = myB.getName();

            return myAName.compareTo(myBName);
        }
    }

    private static InetAddress getValidAddress(NetworkInterface anIn) {
        Enumeration myAddrs = anIn.getInetAddresses();

        while (myAddrs.hasMoreElements()) {
            InetAddress myAddr = (InetAddress) myAddrs.nextElement();

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

    public static InetAddress getWorkableInterface() throws Exception {
        return _workableAddress;
    }

    public static InetAddress getBroadcastAddress() throws Exception {
        byte[] myAddr = _workableAddress.getAddress();

        myAddr[3] = (byte) 255;

        return InetAddress.getByAddress(myAddr);
    }

    public static int getAddressSize() {
        return 4;
    }

    public static IoBuffer encode(InetSocketAddress anAddress, IoBuffer aBuffer) {
        aBuffer.putInt(anAddress.getPort());

        return aBuffer;
    }

    public static InetSocketAddress decode(IoBuffer aBuffer) throws UnknownHostException {
        int myPort = aBuffer.getInt();

        return new InetSocketAddress(_workableAddress, myPort);
    }
}
