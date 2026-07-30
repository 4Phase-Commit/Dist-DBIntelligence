package it.unitn.ds.cases;

import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica.Crash.Type;
import it.unitn.ds.Client;

/**
 * Case in which the coordinator crashes after issuing an UPDATE
 * message to some, but not all replicas.
 * This is first run with enough UPDATEs sent to reach the quorum,
 * then without.
 * <br>
 * <p>
 * Expectations:
 * <ul>
 * <li>A new leader is elected</li>
 * <li>The acknowledged update is recovered and re-attempted by the new
 * coordinator</li>
 * <li>Upon the second crash, a new coordinator is elected</li>
 * <li>The partially disseminated update is restored by the new coordinator
 * <li>
 * </ul>
 * </p>
 */
public class CoordinatorCrashAfterSomeUpdate extends AbstractCase {
    public CoordinatorCrashAfterSomeUpdate(int numReplicas, int coordinatorId) {
        super(numReplicas, coordinatorId);
    }

    @Override
    protected void Execute() {
        ActorRef client = system.actorOf(
                Client.props(
                        1000,
                        2000,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        SendCrash(0, startingCoordinator, Type.Update, N_REPLICAS / 2 + 2); // + 2 to also count the reception of the update on itself

        SendWrite(1000, client, 1, 1, 100);
        SendRead(4000, client, 1, 1);
    }
}
