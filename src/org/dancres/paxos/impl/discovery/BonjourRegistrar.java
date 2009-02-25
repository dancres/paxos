package org.dancres.paxos.impl.discovery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.dnssd.BrowseListener;
import com.apple.dnssd.DNSSD;
import com.apple.dnssd.DNSSDException;
import com.apple.dnssd.DNSSDRegistration;
import com.apple.dnssd.DNSSDService;
import com.apple.dnssd.RegisterListener;
import com.apple.dnssd.ResolveListener;
import com.apple.dnssd.TXTRecord;

public class BonjourRegistrar implements Registrar, BrowseListener, ResolveListener {
    private Logger _logger;
    private ArrayList _hosts = new ArrayList();
    
    BonjourRegistrar() {
        _logger = LoggerFactory.getLogger(getClass());
    }
    
	public void register(InetSocketAddress anAddress) throws IOException {
		try {
			DNSSD.register(null, Registrar.TYPE, anAddress.getPort(), new RegisterImpl());
		} catch (DNSSDException anE) {
			_logger.error("Register failed", anE);
			
			throw new IOException("Register failed");
		}
	}
	
	public HostDetails[] find(long aTimePeriod) throws IOException {
		synchronized(this) {
			_hosts.clear();
		}
		
		try {
			DNSSD.browse(Registrar.TYPE, this);
		} catch (DNSSDException anE) {
			_logger.error("Browse failed", anE);
			
			throw new IOException("Browse failed");
		}
		
		try {
			Thread.sleep(aTimePeriod);
		} catch (InterruptedException anE) {
			_logger.error("Failed to wait for browse to complete", anE);
		}
		
		ArrayList myResults;
		
		synchronized(this) {
			myResults = (ArrayList) _hosts.clone();
		}
		
		HostDetails[] myDetails = new HostDetails[myResults.size()];
		return (HostDetails[]) myResults.toArray(myDetails);
	}
	
	private class RegisterImpl implements RegisterListener {
		public void serviceRegistered(DNSSDRegistration aReg, int aFlags,
				String aServiceName, String aRegType, String aDomain) {
			_logger.info("Registered: " + aServiceName + ", " + aRegType + ", " + aDomain);
		}

		public void operationFailed(DNSSDService aaService, int anErrorCode) {
			_logger.info("Registration failed: " + anErrorCode);
		}
	}

	public void serviceFound(DNSSDService aService, int aFlags, int anIntIndex, String aServiceName,
			String aRegType, String aDomain) {
		try {
			DNSSD.resolve(0, anIntIndex, aServiceName, aRegType, aDomain, this);
		} catch (DNSSDException anE) {
			_logger.error("Resolve failed", anE);
		} finally {
			aService.stop();
		}
	}

	public void serviceLost(DNSSDService aService, int aFlags, int anIntIndex, String aServiceName,
			String aRegType, String aDomain) {
		aService.stop();
	}

	public void operationFailed(DNSSDService aService, int anErrorCode) {
		_logger.info("Registration failed: " + anErrorCode);		
	}

	public void serviceResolved(DNSSDService aService, int aFlags, int anIntIndex,
			String aFullName, String aHostName, int aPort, TXTRecord aDescription) {
		
		synchronized(this) {
			_hosts.add(new HostDetails(aHostName, aPort));
		}
	}
}
