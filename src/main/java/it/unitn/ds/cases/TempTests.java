package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

public class TempTests extends AbstractCase {
    public TempTests(String systemName, int numReplicas, int coordinatorId) {
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

        SendCrash(0, replicas.get(0), Type.Now, 0);
        SendCrash(0, replicas.get(1), Type.Now, 0);
        SendCrash(0, replicas.get(2), Type.Now, 0);

        SendWrite(0, client, 6, 1, 10);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
