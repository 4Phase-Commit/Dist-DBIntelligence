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
    public static final int REQUEST_FORWARD_TIMEOUT = 2000;
    public static final int WRITEOK_TIMEOUT = 2000;
    public static final int SYNCHRONIZAZION_TIMEOUT = 2000;

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
    private Cancellable restoreTimeout;
    private final Queue<Cancellable> fowardTimeouts;
    private final Queue<Cancellable> writeokTimeouts;
    private final Queue<Cancellable> electionAckExpireTimers;

    private final List<List<Update>> coordinatorPendingRecovery;
    private final Stack<AppliedUpdate> history;

    public record WriteReq(ActorRef client, AbstractClient.WriteRequest request) {
    }

    private enum State {
        NORMAL,
        ELECTION
    }

    private State state = State.NORMAL;

    private final Queue<WriteReq> writeRequests;
    private final Queue<Serializable> requests;
    private final Queue<Update> pendingUpdates;

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
        writeRequests = new ArrayDeque<>();
        requests = new ArrayDeque<>();
        history = new Stack<>();
        pendingUpdates = new ArrayDeque<>();
        electionAckExpireTimers = new ArrayDeque<>();
        fowardTimeouts = new ArrayDeque<>();
        writeokTimeouts = new ArrayDeque<>();
        coordinatorPendingRecovery = new ArrayList<>();

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
        debug("crashed");
        // cancel all timeouts
        CancelTimeout(fowardTimeouts);
        CancelTimeout(writeokTimeouts);
        CancelTimeout(electionAckExpireTimers);
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(electionTimeout);
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

        debug("am i the coordinator ?" + amICoordinator);

        if (!amICoordinator)
            return;
        beginHeartBeat();

    }

    private void beginHeartBeat() {
        debug("Begin Heartbeat");
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
        debug("Stop Heartbeat");
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
            OnCanCrashType(msg);

            if (isElectionFirstPhase || hasCrashed)
                return;
            if (entry.getKey() == id && !toMyself)
                continue;
            else if (entry.getKey() == id && toMyself) {
                // Skip network delays if sending to itself
                entry.getValue().tell(msg, getSelf());
                continue;
            }

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

        debug("send election from " + msgID + " to " + dst + " " + e);

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
            List<AppliedUpdate> listOfUpdatesToApply = IntStream.range(0, historyDiff)
                    .mapToObj(i -> history.get(history.size() - 1 - i))
                    .toList(); // immutable for transmission
            tell(new Synchronization(listOfUpdatesToApply, id), replicas.get(entry.getKey()));
        }
    }

    private class Drain implements Serializable {
    }

    private class ReplicaPendingUpdates implements Serializable {
        List<Update> pending;

        public ReplicaPendingUpdates(Queue updateQueue) {
            pending = Collections.unmodifiableList(new ArrayList<>(updateQueue));
        }
    }

    private class PendingRestore implements Serializable {
        List<Update> toRestore;

        public PendingRestore(List<Update> updates) {
            toRestore = Collections.unmodifiableList(new ArrayList<>(updates));
        }
    }

    private class RestoreTimeout implements Serializable {
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
                .match(ReplicaPendingUpdates.class, this::onReplicaPendingUpdates)
                .match(PendingRestore.class, this::onPendingRestore)
                .match(RestoreTimeout.class, this::onRestoreTimeout)
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
                    onUpdate(msg);
                })
                .match(AbstractClient.ReadRequest.class, this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(Drain.class, this::onDrain)
                .match(UpdateRequest.class, this::onUpdateRequets)
                .match(UpdateACK.class, this::onUpdateACK)
                .match(WriteOK.class, msg -> {
                    OnCanCrashType(msg);
                    onWriteOK(msg);
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

    private void onWriteOK(WriteOK writeOK) {
        if (hasCrashed) {
            return;
        }

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
        callbackOnUpdateApplied(update.request.index, update.request.value);

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
    private void onUpdateACK(UpdateACK updateACK) {
        if (hasCrashed) {
            return;
        }

        Logger.log(String.format(
                "[Replica %d] UpdateACK (%d) from replica: %s",
                this.id,
                updateACK.id,
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

    private void onReadRequest(AbstractClient.ReadRequest request) {
        if (hasCrashed) {
            return;
        }

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

    /**
     * Adds new write requests from clients to the {@code writeRequests} queue
     */
    private void onWriteRequest(AbstractClient.WriteRequest request) {
        if (hasCrashed) {
            return;
        }

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
                "[Replica %d] WRITE request (%d, %d) from %s, adding to queue",
                this.id,
                request.index,
                request.value,
                client.path().name()));

        writeRequests.add(new WriteReq(client, request));
        getSelf().tell(new Drain(), getSelf());
    }

    /**
     * Function that handles write requests from the {@code writeRequests} queue
     */
    private void onDrain(Drain d) {
        if (state != State.NORMAL || writeRequests.isEmpty()) {
            return;
        }

        WriteReq r = writeRequests.poll();

        Logger.log(String.format(
                "[Replica %d] WRITE request (%d, %d) from %s, sending to the coordinator",
                this.id,
                r.request.index,
                r.request.value,
                r.client.path().name()));

        requests.add(r.request);

        // The extra step with the update request is needed to forward the client's
        // ActorRef so we can know who to send the result to.
        Update update = new Update(null, r.request, r.client);

        if (id == currentCoordinator) {
            // Skip network delay for self messages
            getSelf().tell(new UpdateRequest(update), getSelf());
        } else {
            tell(new UpdateRequest(update), replicas.get(currentCoordinator));
        }

        fowardTimeouts.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(REQUEST_FORWARD_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));

        if (!writeRequests.isEmpty()) {
            getSelf().tell(new Drain(), getSelf());
        }
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
        if (hasCrashed) {
            return;
        }

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
    private void onUpdate(Update update) {
        if (hasCrashed) {
            return;
        }

        Logger.log(String.format(
                "[Replica %d] UPDATE from coordinator: %d",
                this.id,
                this.currentCoordinator));
        CancelTimeout(fowardTimeouts.poll());
        pendingUpdates.add(update);
        if (currentCoordinator == id) {
            // Skip network delay for self message
            getSelf().tell(new UpdateACK(update.id), getSelf());
        } else {
            tell(new UpdateACK(update.id), replicas.get(currentCoordinator));
        }
        writeokTimeouts.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));
    }

    private void OnElectionACKTimeout(ElectionACKTimeout electionACKTimeout) {
        int crashedReplica = electionACKTimeout.currentElection.toReplica;
        debug("did NOT received ElectionACK in time by " + crashedReplica);

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

    // add seqno to 0
    private void OnSynchronization(Synchronization synchronization) {
        debug(synchronization.newCoordinator + " is the new leader");

        CancelTimeout(electionTimeout); // delete sync timeout
        epoch++;
        updateSEQN = 0;
        currentCoordinator = synchronization.newCoordinator;
        debug("must apply these updates " + synchronization.updates);

        // Assuming all updates are ordered already
        for (AppliedUpdate u : synchronization.updates) {
            history.push(u);
            locations[u.update.request.index] = u.update.request.value;
            callbackOnUpdateApplied(u.update.request.index, u.update.request.value);
        }

        // Immutability is handled by the message class
        tell(new ReplicaPendingUpdates(pendingUpdates), replicas.get(currentCoordinator));
        restoreTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create((long) SYNCHRONIZAZION_TIMEOUT * (indexOfReplica(id) + 1),
                        TimeUnit.MILLISECONDS),
                getSelf(),
                new RestoreTimeout(),
                getContext().system().dispatcher(),
                getSelf());
    }

    /**
     * After a SYNCHRONIZATION, replicas send their pendingUpdates queues to
     * the coordinator with the goal to restore updates that may have been
     * acknowledged by a quorum before the previous coordinator crashed.
     *
     * The coordinator selects the eligible updates and sends them back to the
     * replicas.
     */
    private void onReplicaPendingUpdates(ReplicaPendingUpdates updates) {
        coordinatorPendingRecovery.add(updates.pending);

        if (coordinatorPendingRecovery.size() >= (replicas.size() / 2) + 1) {
            Map<Integer, Update> unique = new LinkedHashMap<>();
            coordinatorPendingRecovery.stream()
                    .flatMap(List::stream)
                    .forEach(update -> unique.putIfAbsent(update.id, update));

            Set<Integer> appliedIds = history.stream()
                    .map(a -> a.update.id)
                    .collect(Collectors.toSet());

            List<Update> toPropagate = unique.values().stream()
                    .filter(u -> !appliedIds.contains(u.id))
                    .toList();

            // Update the next id with the max one + 1
            this.nextUpdateId = toPropagate.stream()
                    .mapToInt(update -> update.id)
                    .max()
                    .orElse(history.isEmpty() ? 0 : history.get(history.size() - 1).update.id) + 1;

            log("RESTORING pending updates: " + toPropagate + " with nextUpdateId=" + nextUpdateId);

            broadcast(new PendingRestore(toPropagate), false);

            // Apply updates to self
            for (Update u : toPropagate) {
                AppliedUpdate a = new AppliedUpdate(u, epoch, updateSEQN++);
                history.add(a);
                locations[a.update.request.index] = a.update.request.value;
                callbackOnUpdateApplied(a.update.request.index, a.update.request.value);
            }

            // End of protocol
            getContext().become(createReceive());
            state = State.NORMAL;
            getSelf().tell(new Drain(), getSelf());
            beginHeartBeat();
        }
    }

    /**
     * Receive the new list of pending updates to be restored from the
     * new coordinator.
     * <p>
     * This is safe because:
     * <ul>
     * <li>If an update has reached the quorum there is at least one alive replica
     * that has it in the pending queue</li>
     * <li>If the coordinator crashed before sending the updates to restore then a
     * correct replica will still have them in the pending queue and will send them
     * to the new coordinator</li>
     * <li>If the coordinator crashes after sending the updates to restore then the
     * receiving replica will apply them and
     * will propagate them after winning the next election</li>
     * </ul>
     * </p>
     */
    private void onPendingRestore(PendingRestore restore) {
        CancelTimeout(restoreTimeout);

        // Apply updates
        for (Update u : restore.toRestore) {
            AppliedUpdate a = new AppliedUpdate(u, epoch, updateSEQN++);
            history.add(a);
            locations[a.update.request.index] = a.update.request.value;
            callbackOnUpdateApplied(a.update.request.index, a.update.request.value);
        }

        // Switch back to normal context
        getContext().become(createReceive());
        state = State.NORMAL;
        getSelf().tell(new Drain(), getSelf());

        pendingUpdates.clear();
        listenForHeartBeat();
    }

    private void onRestoreTimeout(RestoreTimeout timeout) {
        log("Restore timeout, coordinator crashed");
        OnCrashedCoordinator(new CoordinatorCrashed(currentCoordinator));
    }

    private void OnElectionACK(ElectionACK electionACK) {
        debug("received ElectionACK by " + getSender());
        CancelTimeout(electionAckExpireTimers.poll());
    }

    private void OnElection(Election election) {
        CancelTimeout(electionTimeout);
        debug("election ID: " + election.id + " received " + election);
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
            debug("can elect");
            isElectionFirstPhase = false;
            int newCoordinator = getNewCoordinatorId(election.updates);
            if (newCoordinator == id) { // elect me as the leader
                debug("is the leader");
                amICoordinator = true;
                currentCoordinator = id;
                epoch++;
                updateSEQN = 0;
                sendSyncUpdates(election);
                replicas.keySet().retainAll(election.updates.keySet()); // update the replica set
            } else { // am not the leader to pass to the next one
                debug("Cannot be coordinator but " + newCoordinator + " should be");
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
        debug("election timed out");
        beginElection();
    }

    private void OnElectionStart(ElectionStarted electionStarted) {
        if (isElectionFirstPhase)
            return;
        debug("from " + electionStarted.replicaId + " the election is started");
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
            debug("begun the election");
            beginElection(); // the first must send the election directly
        }

        //// NOTE: this is the simples implementation where every one send the election
        //// msg
        // int nextReplica = getNextReplicaIdInRing(id);
        // ActorRef dst = replicas.get(nextReplica);
        // Election e = new Election(Map.of(id,new
        //// LastUpdate(epoch,updateSEQN)),nextReplica,id);
        //
        // debug("send election from "+id+" to " + dst + " " + e);
        // tell(e,dst);
        // electionAckExpireTimers.add(getContext().system().scheduler().scheduleOnce(
        //// // ack timeout
        // Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        // getSelf(),
        // new ElectionACKTimeout(e),
        // getContext().system().dispatcher(),
        // getSelf()));
    }

    // if electionstarted return;
    private void OnCrashedCoordinator(CoordinatorCrashed coordinatorCrashed) {
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(fowardTimeouts);
        CancelTimeout(writeokTimeouts);
        getContext().become(electionRecive());
        state = State.ELECTION;
        debug("the coordinator crashed");
        replicas.remove(currentCoordinator); // remove current coordinator (REMOVE THIS BECAUSE IS REDUNTANT)
        broadcast(new ElectionStarted(id, currentCoordinator), true);
    }

    private void OnHeartBeat(HeartBeat heartBeat) {
        debug("received heartbeat from coordinator " + heartBeat.currentCoordinator);

        CancelTimeout(heartbeatExpireTimer);
        listenForHeartBeat();
    }

}
