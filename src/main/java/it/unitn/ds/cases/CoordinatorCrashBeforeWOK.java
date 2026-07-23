package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

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
                        1000,
                        2000,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        SendCrash(0, startingCoordinator, Type.Update, 3);
        SendRead(0, client, 1, 1);
        SendWrite(1000, client, 1, 1, 30);
        SendWrite(1000, client, 1, 1, 40);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
