Paxos Implementation
====================

Provides a persistent log type construct. Not to be used for high-reliability 
systems at this point.

An example for using the framework can be found in the package `org.dancres.paxos.test.rest`.

`Backend.java` implements a basic key/value store accessed via a RESTful API. It uses most
features of the framework including clustering, checkpointing and recovery.

Detailed notes for the implementation of a service that uses the framework can be found in
the javadoc for `org.dancres.paxos.Paxos`

 Note: Not to be used for high-reliability systems until the LongTerm (see 'LongTerm.java')
 failure-simulation test is fully implemented and has run successfully across a substantial 
 set of sequences.

To run the long-term test:

mvn clean compile
mvn -Dmdep.outputFile=cp.txt dependency:build-classpath
java -classpath $(cat cp.txt):target/classes:target/test-classes  -Djava.util.logging.config.file=target/test-classes/logging.properties  org.dancres.paxos.test.longterm.Main --calibrate --memory --cycles=100000

If you're on a Mac and running without a network connection, you may need to add a multicast route"

sudo route add -net 224.0.0.1/32 -interface lo0
