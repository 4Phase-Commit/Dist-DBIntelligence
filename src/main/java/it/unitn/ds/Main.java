package it.unitn.ds;

import it.unitn.ds.cases.AllWriteOkCrash;
import it.unitn.ds.cases.CoordinatorCrashAfterSomeWriteOk;
import it.unitn.ds.cases.CorrectRW;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        // -------------------------------- COORDINATOR TESTS
        // -------------------------------- //

        // new ReplicaCrashDuringElection(5,0); // race condition if election timeout
        // multiplier and sync timeout are low because i have a lot of replicas and if i
        // dont remove the one crashed i waste time

        // new ReplicaCrashBeforeFirstElection(5,1);

        // new CoordinatorCasualCrash(3, 0);

        // new CoordinatorCrashAfterElection(5,1);

        // new CoordinatorCrashBeforeUpdate(5,1);

        // CoordinatorCasualCrash coordinatorCasualCrash = new
        // CoordinatorCasualCrash("SimpleCoordinatorCrash",5,0);
        // coordinatorCasualCrash.run();

        // Testing a client execution and r/w logic for replicas
        // CorrectRW correctRW = new CorrectRW("CorrectRW", 4, 0);
        // correctRW.run();

        // new TempTests(7, 0);
        // new CorrectRW(7, 0);
        // new AllWriteOkCrash(5, 0);
        new CoordinatorCrashAfterSomeWriteOk(7, 0);

        // ClientRequestToCrashedReplica reqToCrashedReplica = new
        // ClientRequestToCrashedReplica("ReqToCrashed", 5, 0);
        // // reqToCrashedReplica.run();

        // TODO: work in progress
        // CoordinatorCrashBeforeWOK coordinatorCrashBeforeWOK = new
        // CoordinatorCrashBeforeWOK("CoordinatorCrashBeforeWOK",
        // 4, 0);
        // coordinatorCrashBeforeWOK.run();

        // A coordinator crashes after some (but not all) WRITEOK messages
        // CoordinatorCrashAfterSomeWriteOk execCase = new
        // CoordinatorCrashAfterSomeWriteOk("SomeWOK", 4, 0);
        // execCase.run();

        // CoordinatorCrashAfterWriteReq execCase = new
        // CoordinatorCrashAfterWriteReq("AfterReq", 4, 0);
        // execCase.run();

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
