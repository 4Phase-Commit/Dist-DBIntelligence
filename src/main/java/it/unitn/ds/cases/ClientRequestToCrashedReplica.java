package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

public class ClientRequestToCrashedReplica extends AbstractCase {
    public ClientRequestToCrashedReplica(String systemName, int numReplicas, int coordinatorId) {
        super(systemName, numReplicas, coordinatorId);
    }

    @Override
    public void run() {
        ActorRef client = system.actorOf(
                Client.props(
                        1,
                        2,
                        Optional.of(replicas.get(1))),
                "client1");

        SendCrash(0, replicas.get(1), Type.Now, 0);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);
        SendRead(0, client, 1, 1);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
