package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;

/**
 * Case in which the coordinator crashes after issuing only one
 * WRITEOK message excluding the one sent to itself, then the replica
 * that receives it also crashes.
 * <br>
 * <p>
 * Expectations:
 * <ul>
 * <li>A new leader is elected</li>
 * <li>The acknowledged update is recovered by the new coordinator</li>
 * </ul>
 * </p>
 */
public class AllWriteOkCrash extends AbstractCase {
    public AllWriteOkCrash(String systemName, int numReplicas, int coordinatorId) {
        super(systemName, numReplicas, coordinatorId);
    }

    @Override
    public void run() {
        // TODO: fix once crash management is changed
        ActorRef client = system.actorOf(
                Client.props(
                        1,
                        2,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        // Send crash message
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                startingCoordinator,
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 2),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                replicas.get(1),
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Election, 2),
                system.dispatcher(),
                ActorRef.noSender());

        // Send write
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(2), 1, 30),
                system.dispatcher(),
                ActorRef.noSender());

        // Reads to check that the update was propagated successfully
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(3),
                client,
                new Client.SendReadMessage(replicas.get(1), 1),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(3),
                client,
                new Client.SendReadMessage(replicas.get(2), 1),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(3),
                client,
                new Client.SendReadMessage(replicas.get(3), 1),
                system.dispatcher(),
                ActorRef.noSender());
        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
