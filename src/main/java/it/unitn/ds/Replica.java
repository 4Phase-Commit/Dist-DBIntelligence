package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Replica extends AbstractReplica {
    private static final int HEARTBEAT_INTERVAL_MS = 200;
    private static final int HEARTBEAT_TIMEOUT_MS = 300;
    private static final int ELECTIONACK_TIMEOUT_MS = 3000;
    public static final int ELECTION_TIMEOUT_MULTIPLIER = 100;
    public static final int REQUESTFORWARD_TIMEOUT = 200;
    public static final int WRITEOK_TIMEOUT = 200;

    private boolean amICoordinator;
    private int currentCoordinator;
    private TreeMap<Integer, ActorRef> replicas;
    private Map<Integer, Cancellable> heartbeatSchedulers;
    private boolean isElectingStarted;
    private Cancellable heartbeatExpireTimer;
    private Cancellable electionTimeout;
    private final Queue<Cancellable> fowardTimeout;
    private final Queue<Cancellable> writeokTimeout;
    private final Queue<Cancellable> electionAckExpireTimer;
    private int msgBeforeCrash;
    private Queue<Update> updates;
    private int epoch;
    private int updateSEQN;
    private boolean hasCrashed;
    private int updateACKCount;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        epoch = 0;
        updateSEQN = 0;
        updates = new ArrayDeque<>();
        electionAckExpireTimer = new ArrayDeque<>();
        fowardTimeout = new ArrayDeque<>();
        writeokTimeout = new ArrayDeque<>();
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        // TODO: implement
        return replicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        // TODO: implement
        if (msgBeforeCrash>=how_to_crash.after_n_messages_of_type) {
            if (amICoordinator) {
                StopHeartBeat();
            }
            hasCrashed = true;
            msgBeforeCrash = 0;
            getContext().become(crashedReceive());
            Logger.debug(id + " crashed");
        }
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        amICoordinator = sysInit.coordinator_id == id;
        currentCoordinator = sysInit.coordinator_id;
        replicas = new TreeMap<>(sysInit.group);
        heartbeatSchedulers = new HashMap<>(replicas.size());
        Logger.debug("I am "+id+" and i am the coordinator "+amICoordinator);

        if (!amICoordinator) return;
        BeginHeartBeat();

    }

    private void BeginHeartBeat() {
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            if (entry.getKey()==id) continue;
            Cancellable scheduler = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.create(1, TimeUnit.SECONDS),
                    Duration.create(HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new SendHeartBeat(entry.getKey(), entry.getValue(),id),
                    getContext().system().dispatcher(),
                    getSelf());
            heartbeatSchedulers.put(entry.getKey(),scheduler);
        }
    }

    private void StopHeartBeat() {
        for (Map.Entry<Integer, Cancellable> entry : heartbeatSchedulers.entrySet()) {
            entry.getValue().cancel();
        }
    }

    private Integer getNextReplicaIdInRing(int currentKey) {
        Integer nextKey = replicas.higherKey(currentKey);
        if (nextKey == null) {
            nextKey = replicas.firstKey();
        }

        return nextKey;
    }

    private Integer getNewCoordinatorId(Map<Integer, LastUpdate> map) {
        Integer maxKey = null;
        LastUpdate absoluteMaxUpdate = null;

        for (Map.Entry<Integer, LastUpdate> entry : map.entrySet()) {
            LastUpdate currentListMax = entry.getValue();
            if (currentListMax == null) {
                continue;
            }

            if (absoluteMaxUpdate == null || currentListMax.compareTo(absoluteMaxUpdate) > 0) {
                absoluteMaxUpdate = currentListMax;
                maxKey = entry.getKey();
            }
            if (currentListMax.compareTo(absoluteMaxUpdate) == 0 && maxKey < entry.getKey()) {
                absoluteMaxUpdate = currentListMax;
                maxKey = entry.getKey();
            }
        }
        return maxKey;
    }

    private void CancelTimeout(Cancellable timeout) {
        if (timeout!=null) {
            timeout.cancel();
        }
    }

    private void CancelTimeout(Queue<Cancellable> timeouts) {
        for (Cancellable timeout : timeouts)
            if (timeout!=null) {
                timeout.cancel();
            }
    }


    private void broadcast(Serializable msg, boolean toMyself) {
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            if (isElectingStarted || hasCrashed) return;
            if (entry.getKey() == id && !toMyself) continue;
            tell(msg,entry.getValue());
        }
    }


    public final Receive crashedReceive() {
        return createBaseReceiveBuilder()
                .matchAny(msg -> {})
                .build();
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(HeartBeat.class,this::OnHeartBeat)
                .match(SendHeartBeat.class,this::OnSendHeartBeat)
                .match(CoordinatorCrashed.class,this::OnCrashedCoordinator)
                .match(ElectionStarted.class,this::OnElectionStart)
                .match(Election.class,this::OnElection)
                .match(ElectionTimeout.class,this::OnElectionTimeout)
                .match(ElectionACK.class,this::OnElectionACK)
                .match(ElectionACKTimeout.class,this::OnElectionACKTimeout)
                .match(Synchronization.class,this::OnSynchronization)
                .match(Update.class,this::OnUpdate)
                .match(AbstractClient.WriteRequest.class,this::OnWriteRequest)
                .match(UpdateACK.class,this::OnUpdateACK)
                .match(WriteOK.class,this::OnWriteOK)
                .build();
    }

    private void OnWriteOK(WriteOK writeOK) {
        Logger.debug("WriteOK recived by replica " + id);
        CancelTimeout(writeokTimeout.poll());
    }

    private void OnUpdateACK(UpdateACK updateACK) {
        Logger.debug("Got the UpdateACK from " + getSender());
        updateACKCount++;
        if (updateACKCount>=(replicas.size()/2)+1) {
            Logger.debug("Coordinator sending the WriteOK to everyone");
            broadcast(new WriteOK(),false);
            updateACKCount=0;
        }
    }

    private void OnWriteRequest(AbstractClient.WriteRequest writeRequest) {
        if (amICoordinator) {
            Logger.debug("WriteRequest detected ill send the Update to replicas");
            broadcast(new Update(writeRequest),false);
        } else {
            Logger.debug("WriteRequest detected ill send it to the coordinator");
            tell(writeRequest,replicas.get(currentCoordinator));
            fowardTimeout.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                    Duration.create(REQUESTFORWARD_TIMEOUT, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new CoordinatorCrashed(currentCoordinator),
                    getContext().system().dispatcher(),
                    getSelf()));
        }
    }

    private void OnUpdate(Update update) {
        Logger.debug(id +" recived update from the coordinator " + currentCoordinator);
        CancelTimeout(fowardTimeout.poll());
        updates = Stream.concat(updates.stream(), Stream.of(update)).collect(Collectors.toCollection(ArrayDeque::new));
        updateSEQN++;
        tell(new UpdateACK(),replicas.get(currentCoordinator));
        writeokTimeout.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));
    }

    private void OnElectionACKTimeout(ElectionACKTimeout electionACKTimeout) {
        Logger.debug(id + " did NOT received ElectionACK in time");
        int nextReplica = getNextReplicaIdInRing((id + 1) % replicas.size());
        ActorRef dst = replicas.get(nextReplica);
        Election e = new Election(electionACKTimeout.currentElection.updates,nextReplica,electionACKTimeout.currentElection.id);
        tell(e,dst);
        electionAckExpireTimer.poll();
        Logger.debug("send election from "+id+" to " + dst + " " + e);
        electionAckExpireTimer.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionACKTimeout(e),
                getContext().system().dispatcher(),
                getSelf()));
    }

    private void OnSynchronization(Synchronization synchronization) {
        Logger.debug(synchronization.newCoordinator +" is the new leader for " + id );
        epoch++;
        currentCoordinator = synchronization.newCoordinator;
        Queue<Update> updateToApply = Stream.concat(updates.stream(), synchronization.updates.stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting())) // group by how many times the object appears
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() == 1) // filter only the one that appears 1 time
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(ArrayDeque::new));
        Logger.debug(id + " must apply these updates " + updateToApply);
    }

    private void OnElectionACK(ElectionACK electionACK) {
        Logger.debug(id + " received ElectionACK by " + getSender());
        CancelTimeout(electionAckExpireTimer.poll());
    }


    private void OnElection(Election election) {
        CancelTimeout(electionTimeout);
        Logger.debug(id + " election ID: " + election.id + " received " + election);
        tell(new ElectionACK(),getSender()); // ack for the last node
        if (!election.updates.containsKey(id)) {
            int nextReplica = getNextReplicaIdInRing(id);
            ActorRef dst = replicas.get(nextReplica);
            Map<Integer,LastUpdate> newUpdates = Stream.concat(election.updates.entrySet().stream(), Map.of(id,new LastUpdate(epoch,updateSEQN)).entrySet().stream())
                    .collect(Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    )); // for immutable
            Election e = new Election(newUpdates,nextReplica,election.id);
            tell(e,dst);
            Logger.debug("send election from "+id+" to " + dst + " " + e);
            electionAckExpireTimer.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                    Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionACKTimeout(e),
                    getContext().system().dispatcher(),
                    getSelf()));
        } else {
            if (amICoordinator) return;
            Logger.debug(id + " can elect");
            isElectingStarted =false;
            int newCoordinator = getNewCoordinatorId(election.updates);
            if (newCoordinator == id) {
                Logger.debug(id + " is the leader");
                amICoordinator = true;
                epoch++;
                broadcast(new Synchronization(updates,id),false);
                BeginHeartBeat();
                replicas.keySet().retainAll(election.updates.keySet());
            } else {
                Logger.debug("Cannot be coordinator but " + newCoordinator + " should be");
                ActorRef dst = replicas.get(getNextReplicaIdInRing(id));
                tell(election,dst);
                electionAckExpireTimer.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                        Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                        getSelf(),
                        new ElectionACKTimeout(election),
                        getContext().system().dispatcher(),
                        getSelf()));
            }
        }
    }

    private void OnSendHeartBeat(SendHeartBeat sendHeartBeat) {
        tell(new HeartBeat(sendHeartBeat.replicaId,id),sendHeartBeat.replica);
    }

    private void OnElectionTimeout(ElectionTimeout electionTimeout) {
        Logger.debug("election timed out by " + id);
        BeginElection();
    }

    private void BeginElection() {
        int nextReplica = getNextReplicaIdInRing(id);
        ActorRef dst = replicas.get(nextReplica);
        Election e = new Election(Map.of(id,new LastUpdate(epoch,updateSEQN)),nextReplica,id);
        Logger.debug("send election from "+id+" to " + dst + " " + e);
        tell(e,dst);
        electionAckExpireTimer.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionACKTimeout(e),
                getContext().system().dispatcher(),
                getSelf()));

    }

    private void OnElectionStart(ElectionStarted electionStarted) {
        if (isElectingStarted) return;
        Logger.debug("for "+id+" from "+electionStarted.replicaId+" the election is started");
        isElectingStarted = true;
        replicas.remove(currentCoordinator); // remove current coordinator

        int lowestKey = replicas.firstKey();
        if (lowestKey!=id) {
            List<Integer> replicaList = new ArrayList<>(replicas.keySet());
            electionTimeout = getContext().system().scheduler().scheduleOnce( // ack timeout
                    Duration.create((long) ELECTION_TIMEOUT_MULTIPLIER * replicaList.indexOf(id), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionTimeout(),
                    getContext().system().dispatcher(),
                    getSelf());
        } else {
            // can crash here
            Logger.debug(id + " begun the election");
            BeginElection();  // the first must send the election directly
        }

//        int nextReplica = getNextReplicaIdInRing(id);
//        ActorRef dst = replicas.get(nextReplica);
//        Election e = new Election(Map.of(id,new LastUpdate(epoch,updateSEQN)),nextReplica,id);

//        Logger.debug("send election from "+id+" to " + dst + " " + e);
//        tell(e,dst);
//        hasSentElection=true;
//        electionAckExpireTimer.add(getContext().system().scheduler().scheduleOnce( // ack timeout
//                Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
//                getSelf(),
//                new ElectionACKTimeout(e),
//                getContext().system().dispatcher(),
//                getSelf()));
    }

    private void OnCrashedCoordinator(CoordinatorCrashed coordinatorCrashed) {
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(fowardTimeout);
        CancelTimeout(writeokTimeout);
        Logger.debug("for "+id+" the coordinator crashed");
        replicas.remove(currentCoordinator); // remove current coordinator
        broadcast(new ElectionStarted(id,currentCoordinator),true);
    }

    private void OnHeartBeat(HeartBeat heartBeat) {
        Logger.debug(id+" recived heartbeat from coordinator "+heartBeat.currentCoordinator);

        CancelTimeout(heartbeatExpireTimer);
        heartbeatExpireTimer = getContext().system().scheduler().scheduleOnce(
                Duration.create(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf());
    }

}
