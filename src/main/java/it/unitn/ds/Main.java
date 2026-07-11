package it.unitn.ds;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.ds.AbstractReplica.InitSystem;
import scala.concurrent.duration.Duration;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 4;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                    system.actorOf(
                            Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                                    AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                            "Replica_" + i));
        }

        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }

        // Testing a client execution and r/w logic for replicas
        // TODO: polish for final release once done

        ActorRef client = system.actorOf(
                Client.props(
                        1,
                        2,
                        Optional.of(replicas.get(1))),
                "client1");

        // client.tell(new Client.SendReadMessage(replicas.get(1), 1),
        // ActorRef.noSender());
        // client.tell(new Client.SendWriteMessage(replicas.get(1), 1, 150),
        // ActorRef.noSender());
        // client.tell(new Client.SendReadMessage(replicas.get(1), 1),
        // ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(0),
                client,
                new Client.SendReadMessage(replicas.get(1), 1),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(1),
                client,
                new Client.SendWriteMessage(replicas.get(1), 1, 150),
                system.dispatcher(),
                ActorRef.noSender());

        system.scheduler().scheduleOnce(
                java.time.Duration.ofSeconds(4),
                client,
                new Client.SendReadMessage(replicas.get(0), 1),
                system.dispatcher(),
                ActorRef.noSender());

        // TODO: Create your clients

        // TODO: Implement your main logic related to the client

        // testing heartbeat crash
        // system.scheduler().scheduleOnce(
        // Duration.create(2000, TimeUnit.MILLISECONDS),
        // replicas.get(0),
        // new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now, 0),
        // system.dispatcher(),
        // ActorRef.noSender());

        // test for not receiving the writeok
        // replicas.get(1).tell(new
        // AbstractClient.WriteRequest(0,0,ActorRef.noSender()),replicas.get(0));
        // system.scheduler().scheduleOnce(
        // Duration.create(100, TimeUnit.MILLISECONDS),
        // replicas.get(0),
        // new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now,0),
        // system.dispatcher(),
        // ActorRef.noSender());

        // // test for not starting the updateprotocol

        // replicas.get(1).tell(new
        // AbstractClient.WriteRequest(0,0,ActorRef.noSender()),replicas.get(1));
        // replicas.get(0).tell(new
        // AbstractReplica.Crash(AbstractReplica.Crash.Type.Now,0),replicas.get(0));

        // testing normal behaviour of write request
        // replicas.get(1).tell(new
        // AbstractClient.WriteRequest(0,0,ActorRef.noSender()),replicas.get(1));
        // system.scheduler().scheduleOnce(
        // Duration.create(200, TimeUnit.MILLISECONDS),//??
        // replicas.get(0),
        // new AbstractReplica.Crash(AbstractReplica.Crash.Type.Now,0),
        // system.dispatcher(),
        // ActorRef.noSender());

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
