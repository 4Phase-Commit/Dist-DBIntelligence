package it.unitn.ds;

import it.unitn.ds.cases.AllWriteOkCrash;
import it.unitn.ds.cases.CoordinatorCrashAfterSomeUpdate;
import it.unitn.ds.cases.CoordinatorCrashAfterSomeWriteOk;
import it.unitn.ds.cases.CorrectRW;
import it.unitn.ds.cases.TempTests;

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

        // new TempTests(7, 0);
        new CorrectRW(7, 0);
        // new AllWriteOkCrash(5, 0);
        // new CoordinatorCrashAfterSomeUpdate(7, 0);
        // new CoordinatorCrashAfterSomeWriteOk(7, 0);

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
