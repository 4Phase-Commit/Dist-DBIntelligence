package it.unitn.ds.cases;

import it.unitn.ds.AbstractReplica;

import java.util.Collections;

public class ReplicaCrashBeforeFirstElection extends AbstractCase{
    public ReplicaCrashBeforeFirstElection(int numReplicas, int coordinatorId) {
        super(numReplicas, coordinatorId);
    }

    @Override
    public void Execute() {
        SendCrash(0,replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Now,0);
        int firstReplica = Collections.min(replicas.keySet());
        SendCrash(0,replicas.get(firstReplica), AbstractReplica.Crash.Type.Election,0);
    }
}
