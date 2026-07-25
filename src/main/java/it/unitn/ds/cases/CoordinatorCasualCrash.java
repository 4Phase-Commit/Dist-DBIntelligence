package it.unitn.ds.cases;

import it.unitn.ds.AbstractReplica;

import java.io.IOException;

public class CoordinatorCasualCrash extends AbstractCase{
    public CoordinatorCasualCrash(int numReplicas, int coordinatorId) {
        super( numReplicas, coordinatorId);
    }

    @Override
    protected void Execute() {
        SendCrash(2000,replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Now,0);
    }
}
