package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.ReadResult;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Replica extends AbstractReplica {
    private static final int HEARTBEAT_TIMEOUT_MS = 1100;
    private static final int ELECTIONACK_TIMEOUT_MS = 3000;
    public static final int ELECTION_TIMEOUT_MULTIPLIER = 100;
    public static final int REQUEST_FORWARD_TIMEOUT = 200;
    public static final int WRITEOK_TIMEOUT = 200;
    public static final int SYNCHRONIZAZION_TIMEOUT = 500;

    private boolean amICoordinator;
    private int currentCoordinator;
    private boolean isElectionFirstPhase;
    private int msgBeforeCrash;
    private int epoch;
    private int updateSEQN;
    private boolean hasCrashed;

    private int updateACKCount;
    private int nextUpdateId;
    private int currentUpdateId;
    private boolean coordinatorBusy;
    private Queue<Update> coordinatorUpdateQueue;

    private Crash currentCrash;

    private TreeMap<Integer, ActorRef> replicas;
    private Map<Integer, Cancellable> heartbeatSchedulers;
    private Cancellable heartbeatExpireTimer;
    private Cancellable electionTimeout;
    private final Queue<Cancellable> fowardTimeouts;
    private final Queue<Cancellable> writeokTimeouts;
    private final Queue<Cancellable> electionAckExpireTimers;
    private final Queue<Serializable> requests;
    private final Queue<Update> pendingUpdates;
    private final Stack<AppliedUpdate> history;

    private int[] locations;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        // TODO: implement
        epoch = 0;
        updateSEQN = 0;
        requests = new ArrayDeque<>();
        history = new Stack<>();
        pendingUpdates = new ArrayDeque<>();
        electionAckExpireTimers = new ArrayDeque<>();
        fowardTimeouts = new ArrayDeque<>();
        writeokTimeouts = new ArrayDeque<>();

        locations = new int[POSITIONS_LIST_LENGTH];
        nextUpdateId = 0;
        coordinatorUpdateQueue = new ArrayDeque<>();
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
            ActorRef listener) {
        return Props.create(Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    @Override
    public int getSystemNumberOfActors() {
        return replicas.size();
    }

    @Override
    public void crash(Crash how_to_crash) {
        if (how_to_crash.type == Crash.Type.Now) {
            crashNow();
        } else {
            currentCrash = how_to_crash;
        }
    }

    private void crashNow() {
        Logger.debug(id + " crashed");
        if (amICoordinator) {
            stopHeartBeat();
        }
        hasCrashed = true;
        msgBeforeCrash = 0;
        getContext().become(crashedReceive());
    }

    @Override
    public void initSystem(InitSystem sysInit) {
        // TODO: implement
        amICoordinator = sysInit.coordinator_id == id;
        currentCoordinator = sysInit.coordinator_id;
        replicas = new TreeMap<>(sysInit.group);
        heartbeatSchedulers = new HashMap<>(replicas.size());
        Logger.debug("I am " + id + " and i am the coordinator " + amICoordinator);

        if (!amICoordinator)
            return;
        beginHeartBeat();

    }

    private void beginHeartBeat() {
        Logger.debug("Begin Heartbeat");
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            if (entry.getKey() == id)
                continue;
            Cancellable scheduler = getContext().system().scheduler().scheduleWithFixedDelay(
                    Duration.Zero(),
                    Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new SendHeartBeat(entry.getKey(), entry.getValue(), id),
                    getContext().system().dispatcher(),
                    getSelf());
            heartbeatSchedulers.put(entry.getKey(), scheduler);
        }
    }

    private void stopHeartBeat() {
        Logger.debug("Stop Heartbeat");
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
        if (timeout != null) {
            timeout.cancel();
        }
    }

    private void CancelTimeout(Queue<Cancellable> timeouts) {
        for (Cancellable timeout : timeouts)
            if (timeout != null) {
                timeout.cancel();
            }
    }

    private void broadcast(Serializable msg, boolean toMyself) {
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            if (isElectionFirstPhase || hasCrashed)
                return;
            if (entry.getKey() == id && !toMyself)
                continue;
            tell(msg, entry.getValue());
        }
    }

    private void listenForHeartBeat() {
        heartbeatExpireTimer = getContext().system().scheduler().scheduleOnce(
                Duration.create(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf());
    }

    private void beginElection() {
        sendElection(id, Map.of(id, new LastUpdate(epoch, updateSEQN)), id);
    }

    private void sendElection(int next, Map<Integer, LastUpdate> updates, int msgID) {
        int nextReplica = getNextReplicaIdInRing(next);
        ActorRef dst = replicas.get(nextReplica);
        Election e = new Election(updates, nextReplica, msgID);
        tell(e, dst);
        Logger.debug("send election from " + msgID + " to " + dst + " " + e);
        electionAckExpireTimers.add(getContext().system().scheduler().scheduleOnce( // electionack timeout
                Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionACKTimeout(e),
                getContext().system().dispatcher(),
                getSelf()));
    }

    private int indexOfReplica(int id) {
        List<Integer> replicaList = new ArrayList<>(replicas.keySet());
        return replicaList.indexOf(id);
    }

    private void sendSyncUpdates(Election election) {
        for (Map.Entry<Integer, LastUpdate> entry : election.updates.entrySet()) {
            if (entry.getKey() == id)
                continue;
            int historyDiff = election.updates.get(id).epochSEQN - entry.getValue().epochSEQN;
            List<Update> listOfUpdatesToApply = IntStream.range(0, historyDiff)
                    .mapToObj(i -> history.get(history.size() - 1 - i).update)
                    .toList(); // immutable for transmission
            tell(new Synchronization(listOfUpdatesToApply, id), replicas.get(entry.getKey()));
        }
    }

    public final Receive crashedReceive() {
        return createBaseReceiveBuilder()
                .matchAny(a -> {
                })
                .build();
    }

    public final Receive electionRecive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(ElectionStarted.class, this::OnElectionStart)
                .match(Election.class, msg -> {
                    OnCanCrashType(msg);
                    OnElection(msg);
                })
                .match(ElectionTimeout.class, this::OnElectionTimeout)
                .match(ElectionACK.class, this::OnElectionACK)
                .match(ElectionACKTimeout.class, this::OnElectionACKTimeout)
                .match(Synchronization.class, this::OnSynchronization)
                // add requests
                .matchAny(a -> {
                })
                .build();
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .match(HeartBeat.class, msg -> {
                    OnCanCrashType(msg);
                    OnHeartBeat(msg);
                })
                .match(SendHeartBeat.class, this::OnSendHeartBeat)
                .match(CoordinatorCrashed.class, this::OnCrashedCoordinator)
                .match(ElectionStarted.class, this::OnElectionStart)
                .match(Election.class, msg -> {
                    OnCanCrashType(msg);
                    OnElection(msg);
                })
                .match(ElectionTimeout.class, this::OnElectionTimeout)
                .match(ElectionACK.class, this::OnElectionACK)
                .match(ElectionACKTimeout.class, this::OnElectionACKTimeout)
                .match(Synchronization.class, this::OnSynchronization)
                .match(Update.class, msg -> {
                    OnCanCrashType(msg);
                    OnUpdate(msg);
                })
                .match(AbstractClient.ReadRequest.class, this::OnReadRequest)
                .match(AbstractClient.WriteRequest.class, this::OnWriteRequest)
                .match(UpdateRequest.class, this::onUpdateRequets)
                .match(UpdateACK.class, this::OnUpdateACK)
                .match(WriteOK.class, msg -> {
                    OnCanCrashType(msg);
                    OnWriteOK(msg);
                })
                .build();
    }

    private void OnCanCrashType(Serializable msg) {
        if (currentCrash == null)
            return;

        if (msgBeforeCrash >= currentCrash.after_n_messages_of_type) {
            crashNow();
            return;
        }

        boolean isMatch = switch (currentCrash.type) {
            case Heartbeat -> msg instanceof HeartBeat;
            case Update -> msg instanceof Update;
            case WriteOK -> msg instanceof WriteOK;
            case Election -> msg instanceof Election;
            default -> false;
        };

        if (isMatch) {
            msgBeforeCrash++;
        }
    }

    private void OnWriteOK(WriteOK writeOK) {
        Logger.log(String.format(
                "[Replica %d] WRITEOK from coordinator: %s",
                this.id,
                getSender().path().name()));

        CancelTimeout(writeokTimeouts.poll());
        updateSEQN++;
        Update update = this.pendingUpdates.poll();
        AppliedUpdate updateToBeApplied = new AppliedUpdate(update, epoch, updateSEQN);
        history.push(updateToBeApplied);

        Logger.log(String.format(
                "[Replica %d] Applying update: %s",
                this.id,
                updateToBeApplied));
        Logger.log(String.format(
                "[Replica %d] New history: %s",
                this.id,
                history));

        locations[update.request.index] = update.request.value;

        if (update.request.replica == getSelf()) {
            tell(new AbstractClient.WriteResult(true, update.request.index, update.request.value, this.id),
                    update.client);
        }

    }

    /**
     * Counts ACKs for the current update and broadcasts the WRITEOK message
     * once the quorum is reached.
     *
     * <p>
     * This is assumed to be received by the coordinator only.
     * Once the quorum is reached the replica updates itself.
     *
     * After sending the WRITEOK broadcast it will begin to
     * handle the next update in the {@code coordinatorUpdateQueue}
     * if there is one.
     * </p>
     */
    private void OnUpdateACK(UpdateACK updateACK) {
        Logger.log(String.format(
                "[Replica %d] UpdateACK from replica: %s",
                this.id,
                getSender().path().name()));

        if (currentUpdateId != updateACK.id) {
            return;
        }

        updateACKCount++;

        if (updateACKCount >= (replicas.size() / 2) + 1) {
            Logger.log(String.format(
                    "[Replica %d] Write quorum reached, broadcasting WRITEOK",
                    this.id,
                    getSender().path().name()));

            broadcast(new WriteOK(), true);
            updateACKCount = 0;

            if (coordinatorUpdateQueue.isEmpty()) {
                coordinatorBusy = false;
            } else {
                Update update = coordinatorUpdateQueue.poll();
                currentUpdateId = update.id;
                broadcast(update, true);
            }
        }
    }

    private void OnReadRequest(AbstractClient.ReadRequest request) {
        ActorRef client = getSender();

        Logger.log(String.format(
                "[Replica %d] Received READ request (%d) from %s",
                this.id,
                request.index,
                client.path().name()));

        // Read immediately, return whatever this replica has
        if (request.index >= this.locations.length || request.index < 0) {
            tell(new ReadResult(false, request.index, null, this.id), client);
            Logger.log(String.format(
                    "[Replica %d] READ request (%d) from %s - %s",
                    this.id,
                    request.index,
                    client.path().name(),
                    "FAILED"));
        } else {
            tell(new ReadResult(true, request.index, this.locations[request.index], this.id), client);
            Logger.log(String.format(
                    "[Replica %d] READ request (%d) from %s - %s",
                    this.id,
                    request.index,
                    client.path().name(),
                    "SUCCESS"));
        }
    }

    private void OnWriteRequest(AbstractClient.WriteRequest request) {
        ActorRef client = getSender();

        if (request.index >= this.locations.length || request.index < 0) {
            Logger.log(String.format(
                    "[Replica %d] Invalid WRITE request (%d, %d) from %s, rejecting",
                    this.id,
                    request.index,
                    request.value,
                    client.path().name()));

            tell(new AbstractClient.WriteResult(false, request.index, request.value, this.id), client);
            return;
        }

        Logger.log(String.format(
                "[Replica %d] WRITE request (%d, %d) from %s, sending to the coordinator",
                this.id,
                request.index,
                request.value,
                client.path().name()));

        requests.add(request);

        // The extra step with the update request is needed to forward the client's
        // ActorRef so we can know who to send the result to.
        Update update = new Update(null, request, client);
        tell(new UpdateRequest(update), replicas.get(currentCoordinator));

        fowardTimeouts.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(REQUEST_FORWARD_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));
    }

    /**
     * Enqueues a new update request from a replica. For coordinator use only.
     *
     * <p>
     * Messages enqueued here but not immediatly processed will be handled
     * in {@code Replica.OnUpdateACK}
     * </p>
     */
    private void onUpdateRequets(UpdateRequest updateRequest) {
        Update update = new Update(nextUpdateId++, updateRequest.update.request, updateRequest.update.client);

        Logger.log(String.format(
                "[Replica %d] UPDATE REQUEST from: %s",
                this.id,
                getSender().path().name()));

        if (coordinatorBusy) {
            coordinatorUpdateQueue.add(update);
        } else {
            coordinatorBusy = true;
            currentUpdateId = update.id;
            broadcast(update, true);
        }
    }

    /**
     * Adds an update to the replica's pending list, sends the UpdateACK
     * to the coordinator and creates the corresponding timeout for the
     * WRITEOK.
     */
    private void OnUpdate(Update update) {
        Logger.log(String.format(
                "[Replica %d] UPDATE from coordinator: %d",
                this.id,
                this.currentCoordinator));
        CancelTimeout(fowardTimeouts.poll());
        pendingUpdates.add(update);
        tell(new UpdateACK(update.id), replicas.get(currentCoordinator));
        writeokTimeouts.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));
    }

    private void OnElectionACKTimeout(ElectionACKTimeout electionACKTimeout) {
        int crashedReplica = electionACKTimeout.currentElection.toReplica;
        Logger.debug(id + " did NOT received ElectionACK in time by " + crashedReplica);

        electionAckExpireTimers.poll(); // remove from the list the timeout beacause it is expired

        Map<Integer, LastUpdate> newUpdates;
        if (!isElectionFirstPhase) { // im the second phase and I remove the updates of those that have crashed
                                     // (OPTIMIZATION)
            newUpdates = electionACKTimeout.currentElection.updates.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(crashedReplica))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)); // immutable for
                                                                                                    // transmission
        } else {
            newUpdates = Map.copyOf(electionACKTimeout.currentElection.updates); // immutable for transmission
        }
        sendElection(crashedReplica, newUpdates, electionACKTimeout.currentElection.id);
    }

    private void OnSynchronization(Synchronization synchronization) {
        Logger.debug(synchronization.newCoordinator + " is the new leader for " + id);
        getContext().become(createReceive());
        CancelTimeout(electionTimeout); // delete sync timeout
        epoch++;
        currentCoordinator = synchronization.newCoordinator;
        Logger.debug(id + " must apply these updates " + synchronization.updates);
        // TODO: apply all history
        pendingUpdates.clear(); // all pending updates are lost and not applied ever
        listenForHeartBeat();
    }

    private void OnElectionACK(ElectionACK electionACK) {
        Logger.debug(id + " received ElectionACK by " + getSender());
        CancelTimeout(electionAckExpireTimers.poll());
    }

    private void OnElection(Election election) {
        CancelTimeout(electionTimeout);
        Logger.debug(id + " election ID: " + election.id + " received " + election);
        tell(new ElectionACK(), getSender()); // ack for the last node
        if (!election.updates.containsKey(id)) { // add me to election
            Map<Integer, LastUpdate> newUpdates = Stream
                    .concat(election.updates.entrySet().stream(),
                            Map.of(id, new LastUpdate(epoch, updateSEQN)).entrySet().stream())
                    .collect(Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue)); // immutable for transmission
            sendElection(id, newUpdates, election.id);
        } else {
            if (amICoordinator)
                return; // if I am already the coordinator I return and do nothing
            Logger.debug(id + " can elect");
            isElectionFirstPhase = false;
            int newCoordinator = getNewCoordinatorId(election.updates);
            if (newCoordinator == id) { // elect me as the leader
                Logger.debug(id + " is the leader");
                amICoordinator = true;
                epoch++;
                sendSyncUpdates(election);
                getContext().become(createReceive());
                beginHeartBeat();
                replicas.keySet().retainAll(election.updates.keySet()); // update the replica set
            } else { // am not the leader to pass to the next one
                Logger.debug("Cannot be coordinator but " + newCoordinator + " should be");
                sendElection(id, election.updates, election.id);
                electionTimeout = getContext().system().scheduler().scheduleOnce( // synchronizaion message timeout
                        Duration.create((long) SYNCHRONIZAZION_TIMEOUT * (indexOfReplica(id) + 1),
                                TimeUnit.MILLISECONDS),
                        getSelf(),
                        new ElectionTimeout(),
                        getContext().system().dispatcher(),
                        getSelf());
            }
        }
    }

    private void OnSendHeartBeat(SendHeartBeat sendHeartBeat) {
        tell(new HeartBeat(sendHeartBeat.replicaId, id), sendHeartBeat.replica);
    }

    private void OnElectionTimeout(ElectionTimeout electionTimeout) {
        Logger.debug("election timed out by " + id);
        beginElection();
    }

    private void OnElectionStart(ElectionStarted electionStarted) {
        if (isElectionFirstPhase)
            return;
        Logger.debug("for " + id + " from " + electionStarted.replicaId + " the election is started");
        isElectionFirstPhase = true;
        replicas.remove(currentCoordinator); // remove current coordinator

        int lowestKey = replicas.firstKey();
        if (lowestKey != id) {
            electionTimeout = getContext().system().scheduler().scheduleOnce( // ack timeout
                    Duration.create((long) ELECTION_TIMEOUT_MULTIPLIER * indexOfReplica(id), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionTimeout(),
                    getContext().system().dispatcher(),
                    getSelf());
        } else {
            Logger.debug(id + " begun the election");
            beginElection(); // the first must send the election directly
        }

        //// NOTE: this is the simples implementation where every one send the election
        //// msg
        // int nextReplica = getNextReplicaIdInRing(id);
        // ActorRef dst = replicas.get(nextReplica);
        // Election e = new Election(Map.of(id,new
        //// LastUpdate(epoch,updateSEQN)),nextReplica,id);
        //
        // Logger.debug("send election from "+id+" to " + dst + " " + e);
        // tell(e,dst);
        // electionAckExpireTimers.add(getContext().system().scheduler().scheduleOnce(
        //// // ack timeout
        // Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        // getSelf(),
        // new ElectionACKTimeout(e),
        // getContext().system().dispatcher(),
        // getSelf()));
    }

    private void OnCrashedCoordinator(CoordinatorCrashed coordinatorCrashed) {
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(fowardTimeouts);
        CancelTimeout(writeokTimeouts);
        getContext().become(electionRecive());
        Logger.debug("for " + id + " the coordinator crashed");
        replicas.remove(currentCoordinator); // remove current coordinator
        broadcast(new ElectionStarted(id, currentCoordinator), true);
    }

    private void OnHeartBeat(HeartBeat heartBeat) {
        Logger.debug(id + " recived heartbeat from coordinator " + heartBeat.currentCoordinator);

        CancelTimeout(heartbeatExpireTimer);
        listenForHeartBeat();
    }

}
