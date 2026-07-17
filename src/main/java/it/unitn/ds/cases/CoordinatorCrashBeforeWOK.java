package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;

/**
 * Case in which a coordinator crashes during a write operation
 * after sending out the update messages but before sending
 * any WRITEOK.
 * <p>
 * Expectations:
 * <ul>
 * <li>A new coordinator is elected</li>
 * <li>
 * If enough other replicas have an update in the pending list to reach
 * the quorum then the update is restored (during the SYNCHRONIZATION phase)
 * </li>
 * </ul>
 * </p>
 */
public class CoordinatorCrashBeforeWOK extends AbstractCase {

    public CoordinatorCrashBeforeWOK(String systemName, int numReplicas, int coordinatorId) {
        super(systemName, numReplicas, coordinatorId);
    }

    @Override
    public void run() {
        ActorRef client = system.actorOf(
                Client.props(
                        1,
                        2,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                startingCoordinator,
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, 3),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                client,
                new Client.SendReadMessage(replicas.get(1), 1),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 30),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 40),
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
