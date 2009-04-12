package org.dancres.paxos.impl.util;

import java.net.SocketAddress;
import org.dancres.paxos.impl.core.Address;

public class AddressImpl implements Address {
    private SocketAddress _address;

    public AddressImpl(SocketAddress anAddress) {
        _address = anAddress;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof AddressImpl) {
            AddressImpl myOther = (AddressImpl) anObject;

            return _address.equals(myOther._address);
        }

        return false;
    }

    public String toString() {
        return _address.toString();
    }

    public int hashCode() {
        return _address.hashCode();
    }
}
