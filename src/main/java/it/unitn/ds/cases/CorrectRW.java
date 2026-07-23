package it.unitn.ds.cases;

import java.io.IOException;
import java.util.Optional;

import akka.actor.ActorRef;
import it.unitn.ds.Client;

/**
 * Case in which a client performs a bunch of reads and writes
 * to different replicas in a correct system.
 * <p>
 * Expectations:
 * <ul>
 * <li></li>
 * </ul>
 * </p>
 */
public class CorrectRW extends AbstractCase {
    public CorrectRW(String systemName, int numReplicas, int coordinatorId) {
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

        SendRead(0, client, 1, 1);
        SendWrite(1000, client, 1, 1, 10);
        SendWrite(1000, client, 1, 1, 11);
        SendWrite(1000, client, 1, 1, 12);

        SendWrite(2000, client, 2, 1, 20);

        SendWrite(2000, client, 1, 1, 20);

        SendWrite(3000, client, 3, 1, 30);

        SendRead(4000, client, 0, 1);
        SendRead(4000, client, 0, 1);
        SendRead(4000, client, 0, 1);
        SendRead(4000, client, 1, 1);
        SendRead(4000, client, 2, 1);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
