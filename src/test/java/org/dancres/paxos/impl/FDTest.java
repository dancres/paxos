package org.dancres.paxos.impl;

import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.utils.Builder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class FDTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = new TransportImpl(new FailureDetectorImpl(5000), "node1".getBytes());
        myBuilder.newCoreWith(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000), "node2".getBytes());
        myBuilder.newCoreWith(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }

    @Test public void post() throws Exception {
        _tport1.getFD().pin(FailureDetectorImpl.OPEN_PIN);
        _tport2.getFD().pin(FailureDetectorImpl.OPEN_PIN);

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        Assert.assertTrue(_tport1.getFD().getMembers().getMembers().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMembers().size() == 2);

        Map<InetSocketAddress, FailureDetector.MetaData> myMembers = _tport1.getFD().getMembers().getMembers();

        for (Map.Entry<InetSocketAddress, FailureDetector.MetaData> myEntry : myMembers.entrySet()) {
            if (myEntry.getKey().equals(_tport1.getLocalAddress())) {
                Assert.assertTrue("node1".equals(new String(myEntry.getValue().getData())));
            } else if (myEntry.getKey().equals(_tport2.getLocalAddress())) {
                Assert.assertTrue("node2".equals(new String(myEntry.getValue().getData())));
            } else {
                Assert.fail();
            }
        }
    }

    class StateReceiver implements FailureDetector.StateListener {
        private AtomicReference<FailureDetector.State> _state = new AtomicReference<>();

        public void change(FailureDetector aDetector, FailureDetector.State aState) {
            _state.set(aState);
        }

        FailureDetector.State get() {
            return _state.get();
        }
    }

    @Test public void openPin() throws Exception {
        StateReceiver myReceiver = new StateReceiver();

        _tport1.getFD().addListener(myReceiver);

        Assert.assertEquals(FailureDetector.State.OPEN, myReceiver.get());

        _tport1.getFD().pin(FailureDetectorImpl.OPEN_PIN);
        _tport2.getFD().pin(FailureDetectorImpl.OPEN_PIN);

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        // An open pin is treated like a pin but membership is wildcarded
        //
        Assert.assertEquals(FailureDetector.State.PINNED, myReceiver.get());
        Assert.assertTrue(_tport1.getFD().getMembers().getMembers().size() == 2);
    }

    @Test public void pinListener() throws Exception {
        StateReceiver myReceiver = new StateReceiver();

        _tport1.getFD().addListener(myReceiver);

        Assert.assertEquals(FailureDetector.State.OPEN, myReceiver.get());

        Collection<InetSocketAddress> myAddresses = new LinkedList<>();
        myAddresses.add(_tport1.getLocalAddress());
        myAddresses.add(_tport2.getLocalAddress());

        _tport1.getFD().pin(myAddresses);

        Assert.assertEquals(FailureDetector.State.PINNED, myReceiver.get());
    }

    @Test public void pin() throws Exception {
        // OPEN_PIN allows us to "flood" the FD's with the available transports
        //
        _tport1.getFD().pin(FailureDetectorImpl.OPEN_PIN);
        _tport2.getFD().pin(FailureDetectorImpl.OPEN_PIN);

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        Assert.assertTrue(_tport1.getFD().getMembers().getMembers().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMembers().size() == 2);

        Collection<InetSocketAddress> myMembers = _tport1.getFD().getMembers().getMembers().keySet();

        // Now the FD's are flooded, we can pin them
        //
        _tport1.getFD().pin(myMembers);
        _tport2.getFD().pin(myMembers);

        TransportImpl myTport = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN),
                "node3".getBytes());

        // Should work just fine if it exceptions or breaks the FD underneath, we'll know as we proceed
        //
        myTport.getFD().pin(FailureDetectorImpl.OPEN_PIN);

        // myTport should end up with an FD containing 3 nodes because it is unpinned
        //
        Assembly myAssembly = myTport.getFD().barrier(3).get(10000, TimeUnit.MILLISECONDS);

        Assert.assertNotNull(myAssembly);

        // Both other transports should still have membership of 2 and not mention node3
        //
        Assert.assertTrue(_tport1.getFD().getMembers().getMembers().size() == 2);
        Assert.assertTrue(_tport2.getFD().getMembers().getMembers().size() == 2);

        for (FailureDetector.MetaData c : _tport1.getFD().getMembers().getMembers().values()) {
            System.err.println("FD Meta: " + new String(c.getData()));
        }
        
        Assert.assertFalse(_tport1.getFD().getMembers().getMembers().values().stream().map(m ->
                new String(m.getData())).collect(Collectors.toSet()).contains("node3"));
        Assert.assertFalse(_tport2.getFD().getMembers().getMembers().values().stream().map(m ->
                new String(m.getData())).collect(Collectors.toSet()).contains("node3"));

        myTport.terminate();
    }
}
