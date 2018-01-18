package org.dancres.paxos.test.longterm;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Transport;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

class NodeSet {
    private final Deque<NodeAdmin> _nodes = new ConcurrentLinkedDeque<>();

    private NodeAdmin _currentLeader;

    NodeSet(Deque<NodeAdmin> aNodes) {
        _currentLeader = aNodes.getFirst();
        _nodes.addAll(aNodes);
    }

    void install(NodeAdmin anAdmin) {
        _nodes.add(anAdmin);
    }

    /**
     * TODO: Allow killing of current leader
     */
    Deque<NodeAdmin> getKillableNodes() {
        Deque<NodeAdmin> myNodes = new LinkedList<>(_nodes);
        myNodes.remove(_currentLeader);

        return myNodes;
    }

    boolean validate() {
        // All AL's should be in sync post the stability phase of the test
        //
        Deque<NodeAdmin> myRest = new LinkedList<>(_nodes);
        NodeAdmin myBase = myRest.removeFirst();

        for (NodeAdmin myNA : myRest) {
            if (myNA.getLastSeq() != myBase.getLastSeq())
                return false;
        }

        return true;
    }

    public void updateLeader(InetSocketAddress anAddr) {
        for (NodeAdmin myNA : _nodes) {
            Transport myTp = myNA.getTransport();

            if (myTp.getLocalAddress().equals(anAddr)) {
                _currentLeader = myNA;
                break;
            }
        }
    }

    NodeAdmin getCurrentLeader() {
        return _currentLeader;
    }

    NodeAdmin.Memento terminate(NodeAdmin anAdmin) {
        _nodes.remove(anAdmin);
        return anAdmin.terminate();
    }

    void shutdown() {
        for (NodeAdmin myNA : _nodes)
            myNA.terminate();
    }

    void checkpointAll() {
        Checkpoint.these(_nodes);
    }

    boolean makeCurrent(NodeAdmin anAdmin) {
        return Checkpoint.makeCurrent(anAdmin, _nodes);
    }

    Collection<FailureDetector> getFDs() {
        return _nodes.stream().map(n -> n.getTransport().getFD()).collect(Collectors.toList());
    }
}
