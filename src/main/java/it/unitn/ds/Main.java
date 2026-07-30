package it.unitn.ds;

import it.unitn.ds.cases.*;

public class Main {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        // -------------------------------- COORDINATOR TESTS -------------------------------- //
        // new CoordinatorCasualCrash(3, 0);

        // new CoordinatorCrashAfterElection(5,1);

        // new CoordinatorCrashBeforeUpdate(5,1);

        // new CoordinatorCrashAfterSomeUpdate(7, 0);

        // new CoordinatorCrashAfterSomeWriteOk(7, 0);

        // -------------------------------- REPLICA TESTS -------------------------------- //
        // new ReplicaCrashDuringElection(5,0);

        // new ReplicaCrashBeforeFirstElection(5,1);

        // new AllWriteOkCrash(5, 0);

        // new TempTests(7, 0);

        // new CorrectRW(7, 0);

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
    }

}
