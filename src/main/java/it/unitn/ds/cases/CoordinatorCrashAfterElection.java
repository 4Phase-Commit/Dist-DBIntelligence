package it.unitn.ds.cases;

import it.unitn.ds.AbstractReplica;

import java.util.Collections;

public class CoordinatorCrashAfterElection extends AbstractCase{
    public CoordinatorCrashAfterElection(int numReplicas, int coordinatorId) {
        super( numReplicas, coordinatorId);
    }

    @Override
    protected void Execute() {
        SendCrash(0,replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Now,0);
        int newCoordinator = Collections.max(replicas.keySet());
        SendCrash(0,replicas.get(newCoordinator), AbstractReplica.Crash.Type.Election,2);
    }
}
