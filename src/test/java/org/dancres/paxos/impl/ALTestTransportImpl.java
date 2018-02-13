package org.dancres.paxos.impl;

import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.PicklerImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.net.TestAddresses;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

class ALTestTransportImpl implements Transport {
    private final List<PaxosMessage> _messages = new ArrayList<>();
    private final InetSocketAddress _nodeId;
    private final Transport.PacketPickler _pickler;
    private final InetSocketAddress _broadcast;
    private final MessageBasedFailureDetector _fd;

    ALTestTransportImpl(InetSocketAddress aNodeId) {
        this(aNodeId, TestAddresses.next(), new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
    }
    
    ALTestTransportImpl(InetSocketAddress aNodeId, InetSocketAddress aBroadcastAddress,
                        MessageBasedFailureDetector anFD) {
        _nodeId = aNodeId;
        _pickler = new PicklerImpl(_nodeId);
        _broadcast = aBroadcastAddress;
        _fd = anFD;
    }
    
    public void routeTo(Dispatcher aDispatcher) {
    }

    public FailureDetector getFD() {
        return _fd;
    }

    public void send(Packet aPacket, InetSocketAddress aNodeId) {
        synchronized(_messages) {
            _messages.add(aPacket.getMessage());
            _messages.notifyAll();
        }
    }

    @Override
    public void filterRx(Filter aFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void filterTx(Filter aFilter) {
        throw new UnsupportedOperationException();
    }

    PaxosMessage getNextMsg() {
        synchronized(_messages) {
            while (_messages.size() == 0) {
                try {
                    _messages.wait();
                } catch (InterruptedException anIE) {
                    // Ignore
                }
            }

            return _messages.remove(0);
        }
    }

    public Transport.PacketPickler getPickler() {
        return _pickler;
    }

    public InetSocketAddress getBroadcastAddress() {
        return _broadcast;
    }
    public InetSocketAddress getLocalAddress() {
        return _nodeId;
    }

    public void terminate() {
    }
}

