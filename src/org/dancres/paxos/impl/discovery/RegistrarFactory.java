package org.dancres.paxos.impl.discovery;

public class RegistrarFactory {
	public static Registrar getRegistrar() {
		try {
			// Test for Bonjour/ZeroConf and use it by preference
			//
			Class.forName("com.apple.dnssd.DNSSD");
			
			return new BonjourRegistrar();
		} catch (ClassNotFoundException aCNFE) {
			// Fallback to JmDNS
			//
			return new JmDNSRegistrar();
		}
	}
}
