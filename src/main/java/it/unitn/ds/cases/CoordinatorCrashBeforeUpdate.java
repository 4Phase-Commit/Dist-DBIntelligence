package it.unitn.ds.cases;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.Client;

import java.util.Optional;

public class CoordinatorCrashBeforeUpdate extends AbstractCase{
    public CoordinatorCrashBeforeUpdate(int numReplicas, int coordinatorId) {
        super( numReplicas, coordinatorId);
    }

    @Override
    protected void Execute() {
        ActorRef client = system.actorOf(
                Client.props(
                        1000,
                        2000,
                        Optional.of(replicas.get(1))),
                "client1");

        SendCrash(0, replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Update, 0);

        SendWrite(1000, client, 1, 1, 100);
    }
}
