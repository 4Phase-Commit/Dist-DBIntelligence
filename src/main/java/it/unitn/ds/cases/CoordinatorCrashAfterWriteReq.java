package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

// TODO: Do we want the replica to drop the update instead?
/**
 * Case in which the coordinator crashes after receiving a write request
 * from a replica, before sending any update messages.
 * <br>
 * <p>
 * Expectations:
 * <ul>
 * <li>A new leader is elected</li>
 * <li>The replica sees the update request was dropped and tries sending it
 * again to the new coordinator</li>
 * </ul>
 * </p>
 */
public class CoordinatorCrashAfterWriteReq extends AbstractCase {
    public CoordinatorCrashAfterWriteReq(String systemName, int numReplicas, int coordinatorId) {
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

        SendWrite(0, client, 1, 1, 100);
        SendCrash(0, startingCoordinator, Type.Now, 0);
        SendRead(1000, client, 1, 1);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
