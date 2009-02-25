package org.dancres.paxos.impl.discovery;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import javax.jmdns.JmDNS;
import javax.jmdns.ServiceInfo;

public class JmDNSRegistrar implements Registrar {
	public void register(InetSocketAddress anAddr) throws IOException {
        JmDNS myAdvertiser = JmDNS.create(anAddr.getAddress(), flatten(anAddr.getAddress()));
        
        ServiceInfo myInfo = ServiceInfo.create(Registrar.TYPE + ".local", 
        		"paxos@" + flatten(anAddr.getAddress()), anAddr.getPort(), "");
       
        myAdvertiser.registerService(myInfo);		
	}
	
    private String flatten(InetAddress anAddress) {
    	byte[] myAddr = anAddress.getAddress();
    	long myNodeId = 0;
    	
        for (int i = 0; i < 4; i++) {
            myNodeId = myNodeId << 8;
        	myNodeId |= (int) myAddr[i] & 0xFF;
        }
    	
    	return Long.toHexString(myNodeId);
    }

	public HostDetails[] find(long timePeriod) throws IOException {
		throw new IOException("Not Implemented");
	}	
}
