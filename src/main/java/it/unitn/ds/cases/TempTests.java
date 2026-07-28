package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.Client;
import it.unitn.ds.AbstractReplica.Crash.Type;

public class TempTests extends AbstractCase {
    public TempTests(int numReplicas, int coordinatorId) {
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

        SendWrite(0, client, 1, 1, 11);
        SendWrite(0, client, 1, 1, 12);
        SendWrite(0, client, 1, 1, 13);
        SendWrite(0, client, 1, 1, 14);
        SendWrite(0, client, 1, 1, 15);
        SendWrite(0, client, 1, 1, 16);
        SendWrite(0, client, 1, 1, 17);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
