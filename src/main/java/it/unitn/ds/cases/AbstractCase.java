package it.unitn.ds.cases;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.Client;
import it.unitn.ds.Replica;

/**
 * Class for modeling and running execution cases for the project.
 * Create a subclass that overrides the {@code run} method to create
 * new cases.
 */
public class AbstractCase {
    protected int N_REPLICAS;
    protected int STARTING_COORDINATOR_ID;

    protected final ActorSystem system;
    protected Map<Integer, ActorRef> replicas;

    public AbstractCase(String systemName, int numReplicas, int coordinatorId) {
        N_REPLICAS = numReplicas;
        STARTING_COORDINATOR_ID = coordinatorId;

        system = ActorSystem.create(systemName);

        replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                    system.actorOf(
                            Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                                    AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                            "Replica_" + i));
        }

        InitSystem initMsg = new InitSystem(replicas, STARTING_COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }
    }

    public void SendRead(int delayMillis, ActorRef clientRef, int destinationId, int index) {
        system.scheduler().scheduleOnce(
                java.time.Duration.ofMillis(delayMillis),
                clientRef,
                new AbstractClient.ReadRequest(index, replicas.get(destinationId)),
                system.dispatcher(),
                ActorRef.noSender());
    }

    public void SendWrite(int delayMillis, ActorRef clientRef, int destinationId, int index, int value) {
        system.scheduler().scheduleOnce(
                java.time.Duration.ofMillis(delayMillis),
                clientRef,
                new AbstractClient.WriteRequest(index, value, replicas.get(destinationId)),
                system.dispatcher(),
                ActorRef.noSender());
    }

    public void SendCrash(int delayMillis, ActorRef destination, AbstractReplica.Crash.Type type, int n_messages) {
        system.scheduler().scheduleOnce(
                java.time.Duration.ofMillis(delayMillis),
                destination,
                new AbstractReplica.Crash(type, n_messages),
                system.dispatcher(),
                ActorRef.noSender());
    }

    public void run() {
        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
