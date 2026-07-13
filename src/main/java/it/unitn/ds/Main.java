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
import it.unitn.ds.cases.CoordinatorCrashBeforeWOK;
import it.unitn.ds.cases.CorrectRW;
import scala.concurrent.duration.Duration;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        // Testing a client execution and r/w logic for replicas
        CorrectRW correctRW = new CorrectRW("CorrectRW", 4, 0);
        correctRW.run();

        // TODO: work in progress
        // CoordinatorCrashBeforeWOK coordinatorCrashBeforeWOK = new
        // CoordinatorCrashBeforeWOK("CoordinatorCrashBeforeWOK",
        // 4, 0);
        // coordinatorCrashBeforeWOK.run();

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

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
