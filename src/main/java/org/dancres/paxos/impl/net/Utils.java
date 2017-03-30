package org.dancres.paxos.impl.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;

public class Utils {
    private static final Logger _logger = LoggerFactory.getLogger(Utils.class);

    private static NetworkInterface _workableInterface = null;
    private static InetAddress _workableAddress = null;

    /*
     * Iterate interfaces and look for one that's multicast capable and not a 127 or 169 based address
     */
    static {
        SortedSet<NetworkInterface> myWorkableInterfaces = 
        	new TreeSet<>(new NetworkInterfaceComparator());

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

            for (NetworkInterface myWorkableInterface : myWorkableInterfaces) {
                _logger.debug("Candidate Interface: " + myWorkableInterface);
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

    private static class NetworkInterfaceComparator implements Comparator<NetworkInterface>, Serializable {

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

    public static String getNetworkInterface() {
    	return _workableInterface.getName();
    }

    public static NetworkInterface getWorkableInterface() {
        return _workableInterface;
    }

    public static InetAddress getWorkableInterfaceAddress() {
        return _workableAddress;
    }

    public static InetAddress getBroadcastAddress() {
        return _workableInterface.getInterfaceAddresses().get(0).getBroadcast();
    }

    public static byte[] marshall(InetSocketAddress anAddr) throws Exception {
    	ByteArrayOutputStream myBAOS = new ByteArrayOutputStream();
    	ObjectOutputStream myOOS = new ObjectOutputStream(myBAOS);
    	
    	myOOS.writeObject(anAddr);
    	myOOS.close();
    	
    	return myBAOS.toByteArray();
    }
    
    public static InetSocketAddress unmarshallInetSocketAddress(byte[] aBytes) throws Exception {
    	ByteArrayInputStream myBAIS = new ByteArrayInputStream(aBytes);
    	ObjectInputStream myOIS = new ObjectInputStream(myBAIS);
    	
    	return (InetSocketAddress) myOIS.readObject();
    }
}
