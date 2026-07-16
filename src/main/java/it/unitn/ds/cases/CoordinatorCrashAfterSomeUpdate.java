package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
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
                        1,
                        2,
                        Optional.of(replicas.get(1))),
                "client1");

        ActorRef startingCoordinator = this.replicas.get(STARTING_COORDINATOR_ID);
        // Presumably the highest id will win in absence of newer writes
        ActorRef secondCoordinator = this.replicas.get(this.replicas.size() - 1);

        // Crash messages
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                startingCoordinator,
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, this.N_REPLICAS / 2 + 1), // quorum
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                secondCoordinator,
                // TODO: consider the update recovery from the previous operation in the crash
                // conditions
                new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, (this.N_REPLICAS - 1) / 2), // no quorum
                system.dispatcher(),
                ActorRef.noSender());

        // First write
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 100),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(2),
                client,
                new Client.SendReadMessage(replicas.get(1), 1),
                system.dispatcher(),
                ActorRef.noSender());

        // Second write
        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(4),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 200),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(5),
                client,
                new Client.SendReadMessage(replicas.get(1), 1),
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
