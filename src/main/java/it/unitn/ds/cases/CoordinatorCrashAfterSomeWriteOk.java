
package it.unitn.ds.cases;

import java.io.IOException;
import java.sql.ClientInfoStatus;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

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
                        1000,
                        2000,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        SendCrash(0, startingCoordinator, Type.WriteOK, 3);
        SendWrite(1000, client, 1, 1, 30);

        // Reads to check that the update was propagated successfully
        SendRead(3000, client, 1, 1);
        SendRead(3000, client, 2, 1);
        SendRead(3000, client, 3, 1);

        // New write to check if <e, i> were updated correctly
        SendWrite(4000, client, 3, 1, 40);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
