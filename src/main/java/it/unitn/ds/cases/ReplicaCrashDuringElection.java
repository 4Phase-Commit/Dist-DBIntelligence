package it.unitn.ds.cases;

import it.unitn.ds.AbstractReplica;

import java.util.Random;

public class ReplicaCrashDuringElection extends AbstractCase{
    public ReplicaCrashDuringElection(int numReplicas, int coordinatorId) {
        super( numReplicas, coordinatorId);
    }

    @Override
    protected void Execute() {
        SendCrash(0,replicas.get(STARTING_COORDINATOR_ID), AbstractReplica.Crash.Type.Now,0);
        Random rng = new Random();
        int randomReplica = rng.nextInt(replicas.keySet().stream().filter(r -> r!=STARTING_COORDINATOR_ID).toList().size());

        System.out.println("Replica "+ randomReplica +" is expected to crash");

        SendCrash(0,replicas.get(randomReplica), AbstractReplica.Crash.Type.Election,0);
    }
}
