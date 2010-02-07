package org.dancres.paxos;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.slf4j.Logger;

/**
 * PacketBuffer tracks a collection of PaxosMessages. The collection is ordered by sequence number and secondarily
 * by operation type. PacketBuffer also tracks log offset for each message which is used to compute appropriate
 * checkpoints in log files.
 */
class PacketBuffer {
}
