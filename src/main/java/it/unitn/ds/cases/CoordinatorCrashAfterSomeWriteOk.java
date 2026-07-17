
package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;

/**
 * Case in which the coordinator crashes after issuing some
 * WRITEOK messages for an update but not all.
 * <br>
 * <p>
 * Expectations:
 * <ul>
 * <li>A new leader is elected, among those who have received the WRITEOK
 * (because of the higher epoch/sequence number pair)</li>
 * <li>Other replicas receive the update during the SYNCHRONIZATION phase</li>
 * </ul>
 * </p>
 */
public class CoordinatorCrashAfterSomeWriteOk extends AbstractCase {
    public CoordinatorCrashAfterSomeWriteOk(String systemName, int numReplicas, int coordinatorId) {
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
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 3),
                system.dispatcher(),
                ActorRef.noSender());

        // Send write
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 30),
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

        // New write to check if <e, i> were updated correctly
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(4),
                client,
                new Client.SendWriteMessage(replicas.get(3), 1, 40),
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
