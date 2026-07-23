package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

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
 * <li>The partially disseminated update was no seen by enough replicas and is
 * dropped
 * <li>
 * </ul>
 * </p>
 */
class CoordinatorCrashAfterSomeUpdate extends AbstractCase {
    public CoordinatorCrashAfterSomeUpdate(String systemName, int numReplicas, int coordinatorId) {
        super(systemName, numReplicas, coordinatorId);
    }

    @Override
    public void run() {
        // TODO: fix the crash scheduling once they are changed
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

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
