package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

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
                        1000,
                        2000,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);

        // Send crash message
        SendCrash(0, startingCoordinator, Type.WriteOK, 2);
        SendCrash(0, replicas.get(1), Type.Election, 2);

        // Send write
        SendWrite(1000, client, 2, 1, 30);

        // Reads to check that the update was propagated successfully
        SendRead(3000, client, 1, 1);
        SendRead(3000, client, 2, 1);
        SendRead(3000, client, 3, 1);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
