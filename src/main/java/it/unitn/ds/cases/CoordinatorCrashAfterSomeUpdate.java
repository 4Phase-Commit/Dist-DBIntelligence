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
        // Presumably the highest id will win in absence of newer writes
        ActorRef secondCoordinator = this.replicas.get(this.replicas.size() - 1);

        SendCrash(0, startingCoordinator, Type.Update, N_REPLICAS / 2 + 1);
        // TODO: consider the update recovery from the previous operation in the crash
        // conditions
        SendCrash(0, secondCoordinator, Type.Update, (N_REPLICAS - 1) / 2);

        SendWrite(1000, client, 1, 1, 100);
        SendRead(2000, client, 1, 1);

        SendWrite(4000, client, 1, 1, 200);
        SendRead(5000, client, 1, 1);
    }
}
