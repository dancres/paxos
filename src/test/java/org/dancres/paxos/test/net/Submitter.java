package org.dancres.paxos.test.net;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.Event;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Submitter implements Transport.Filter {
    private static final Logger _logger = LoggerFactory.getLogger(Submitter.class);

    private final Core _core;

    public Submitter(Core aCore) {
        _core = aCore;
    }

    public Transport.Packet filter(Transport aTransport, Transport.Packet aPacket) {
        if (_core.isInitd()) {
            PaxosMessage myMessage = aPacket.getMessage();

            try {
                if (myMessage.getClassifications().contains(PaxosMessage.Classification.CLIENT)) {
                    final InetSocketAddress mySource = aPacket.getSource();

                    Envelope myEnvelope = (Envelope) myMessage;
                    Proposal myProposal = myEnvelope.getValue();

                    _core.submit(myProposal, (VoteOutcome anOutcome) ->
                            _core.getCommon().getTransport().send(
                                    _core.getCommon().getTransport().getPickler().newPacket(new Event(anOutcome)),
                                    mySource));

                    return null;
                }
            } catch (Throwable anE) {
                _logger.error("Unexpected exception", anE);
            }
        }

        return aPacket;
    }
}
