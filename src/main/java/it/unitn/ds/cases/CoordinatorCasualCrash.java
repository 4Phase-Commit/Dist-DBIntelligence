package it.unitn.ds.cases;

import it.unitn.ds.AbstractReplica;

import java.io.IOException;

public class CoordinatorCasualCrash extends AbstractCase{
    public CoordinatorCasualCrash(String systemName, int numReplicas, int coordinatorId) {
        super(systemName, numReplicas, coordinatorId);
    }

    @Override
    public void run() {
        SendCrash(2000,replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Now,0);

        try {
            System.out.println(">>> Press ENTER to continue");
            System.in.read();
        } catch (IOException e) {
        }

        system.terminate();
    }
}
