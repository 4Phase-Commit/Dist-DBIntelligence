package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

public class ClientRequestToCrashedReplica extends AbstractCase {
    public ClientRequestToCrashedReplica(int numReplicas, int coordinatorId) {
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

        SendCrash(0, replicas.get(1), Type.Now, 0);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
    }
}
