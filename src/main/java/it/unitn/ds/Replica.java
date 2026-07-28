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
    private static final int HEARTBEAT_TIMEOUT_MS = 100;
    private static final int ELECTIONACK_TIMEOUT_MS = 200;
    private static final int ELECTION_TIMEOUT_MULTIPLIER = 400;
    private static final int SYNCHRONIZAZION_TIMEOUT = 100;
    private static final int RESTORE_TIMEOUT_MS = 500;
    private static final int REQUEST_FORWARD_TIMEOUT = 500;
    private static final int WRITEOK_TIMEOUT = 500;

    private boolean amICoordinator;
    private boolean isElectionFirstPhase;
    private boolean acceptingUpdateAcks = false;
    private boolean coordinatorBusy;
    private boolean hasCrashed;
    private boolean retryRequests = false;

    private int currentCoordinator;
    private int msgBeforeCrash;
    private int epoch;
    private int updateSEQN;
    private int updateACKCount;
    private int nextUpdateId;

    private TreeMap<Integer, ActorRef> replicas;

    private Cancellable heartbeatTimer;
    private Cancellable heartbeatExpireTimer;
    private Cancellable electionTimeout;
    private Cancellable restoreTimeout;

    private final Queue<Cancellable> fowardTimeouts;
    private final Queue<Cancellable> writeokTimeouts;
    private final Queue<Cancellable> electionAckExpireTimers;

    private final Queue<Update> coordinatorUpdateQueue;
    /**
     * Request received by a client that are yet to be forwarded to the coordinator
     */
    private final Queue<Update> writeRequests;
    private final List<List<Update>> coordinatorPendingRecovery;
    private final Stack<AppliedUpdate> history;
    /** Requests sent to the coordinator but not yet broadcast by it */
    private final Queue<Update> pendingRequests;
    private final Queue<Update> pendingUpdates;

    private final int[] locations;

    private Crash currentCrash;

    public record UpdateId(int replica, int id) {
    }

    private UpdateId currentUpdateId;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        epoch = 0;
        updateSEQN = 0;

        writeRequests = new ArrayDeque<>();
        pendingRequests = new ArrayDeque<>();
        history = new Stack<>();
        pendingUpdates = new ArrayDeque<>();
        electionAckExpireTimers = new ArrayDeque<>();
        fowardTimeouts = new ArrayDeque<>();
        writeokTimeouts = new ArrayDeque<>();
        coordinatorPendingRecovery = new ArrayList<>();
        coordinatorUpdateQueue = new ArrayDeque<>();

        locations = new int[POSITIONS_LIST_LENGTH];

        nextUpdateId = 0;
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
        debug("Crashed");
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
        amICoordinator = sysInit.coordinator_id == id;
        currentCoordinator = sysInit.coordinator_id;
        replicas = new TreeMap<>(sysInit.group);

        debug("Am i the coordinator? " + amICoordinator);

        if (!amICoordinator) {
            listenForHeartBeat();
        } else {
            beginHeartBeat();
        }
    }

    /**
     * Populates the {@code heartbeatSchedulers} map by creating a dedicated
     * scheduler
     * for each replica, allowing heartbeats to be managed independently.
     */
    private void beginHeartBeat() {
        log("Begin Heartbeat");
        heartbeatTimer = getContext().system().scheduler().scheduleWithFixedDelay(
                Duration.Zero(),
                Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
                getSelf(),
                new SendHeartBeat(),
                getContext().system().dispatcher(),
                getSelf());
    }

    /**
     * Clears and shuts down the {@code heartbeatSchedulers} map to stop all active
     * heartbeat intervals.
     */
    private void stopHeartBeat() {
        log("Stop Heartbeat");
        CancelTimeout(heartbeatTimer);
    }

    /**
     * Identifies the next replica in the virtual ring during the election protocol.
     */
    private Integer getNextReplicaIdInRing(int currentKey) {
        Integer nextKey = replicas.higherKey(currentKey);
        if (nextKey == null) {
            nextKey = replicas.firstKey();
        }

        return nextKey;
    }

    /**
     * Determines the ID of the new coordinator after all replicas have populated
     * their metadata in the election message.
     * <p>
     * The elected coordinator is chosen based on the most up-to-date state (highest
     * update sequence/epoch).
     * In the event of a tie, the replica with the highest ID is selected.
     */
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

    /**
     * Cancels and clears a specified timeout.
     */
    private void CancelTimeout(Cancellable timeout) {
        if (timeout != null) {
            timeout.cancel();
        }
    }

    /**
     * Cancels and clears all scheduled timeouts from the queue.
     */
    private void CancelTimeout(Queue<Cancellable> timeouts) {
        while (!timeouts.isEmpty()) {
            Cancellable timeout = timeouts.poll();
            if (timeout != null) {
                timeout.cancel();
            }
        }
    }

    /**
     * Broadcasts a message to all replicas in the cluster.
     *
     * @param toMyself {@code true} if the message should also be sent to this
     *                 replica; {@code false} otherwise
     */
    private void broadcast(Serializable msg, boolean toMyself) {
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            OnCanCrashType(msg);

            if (isElectionFirstPhase || hasCrashed)
                return;

            if (entry.getKey() == id && !toMyself)
                continue;
            else if (entry.getKey() == id) {
                // Skip network delays if sending to itself
                entry.getValue().tell(msg, getSelf());
                continue;
            }

            tell(msg, entry.getValue());
        }
    }

    /**
     * Sets the heartbeat timer for the replica.
     */
    private void listenForHeartBeat() {
        heartbeatExpireTimer = getContext().system().scheduler().scheduleOnce(
                Duration.create(getCoordinatorBeatInterval() + HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf());
    }

    /**
     * Wrapper method to initiate and send the first election message.
     */
    private void beginElection() {
        sendElection(id, Map.of(id, new LastUpdate(epoch, updateSEQN)), id);
    }

    /**
     * Constructs, dispatches, and schedules a timeout for an election message.
     */
    private void sendElection(int nextTo, Map<Integer, LastUpdate> updates, int msgID) {
        int nextReplica = getNextReplicaIdInRing(nextTo);
        ActorRef dst = replicas.get(nextReplica);
        Election e = new Election(updates, nextReplica, msgID);
        tell(e, dst);

        log("Send election from " + id + " to " + dst + " " + e);

        electionAckExpireTimers.add(getContext().system().scheduler().scheduleOnce( // electionack timeout
                Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                getSelf(),
                new ElectionACKTimeout(e),
                getContext().system().dispatcher(),
                getSelf()));
    }

    /**
     * Retrieves the index of a specified replica within the replica map.
     */
    private int indexOfReplica(int replicaId) {
        List<Integer> replicaList = new ArrayList<>(replicas.keySet());
        return replicaList.indexOf(replicaId);
    }

    /**
     * Sends personalized lost updates to each replica following the election of a
     * new coordinator.
     */
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

    public final Receive crashedReceive() {
        return createBaseReceiveBuilder()
                .matchAny(a -> {
                })
                .build();
    }

    public final Receive electionReceive() {
        return createBaseReceiveBuilder()
                .match(ElectionStarted.class, this::OnElectionStart)
                .match(Election.class, msg -> {
                    OnCanCrashType(msg);
                    OnElection(msg);
                })
                .match(ElectionTimeout.class, this::OnElectionTimeout)
                .match(ElectionACK.class, this::OnElectionACK)
                .match(ElectionACKTimeout.class, this::OnElectionACKTimeout)
                .match(Synchronization.class, this::onSynchronization)
                .match(ReplicaPendingUpdates.class, this::onReplicaPendingUpdates)
                .match(PendingRestore.class, this::onPendingRestore)
                .match(RestoreTimeout.class, this::onRestoreTimeout)
                // Still match the read and write requests during election
                .match(AbstractClient.ReadRequest.class, this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(UpdateRequest.class, this::onUpdateRequets)
                .matchAny(a -> {
                })
                .build();
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(HeartBeat.class, msg -> {
                    OnCanCrashType(msg);
                    OnHeartBeat(msg);
                })
                .match(SendHeartBeat.class, this::OnSendHeartBeat)
                .match(CoordinatorCrashed.class, this::OnCrashedCoordinator)
                .match(ElectionStarted.class, this::OnElectionStart)
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
                .matchAny(m -> debug("Ignored message " + m + " from " + getSender().path().name()))
                .build();
    }

    /**
     * Checks if the received message matches the {@code currentCrash} condition.
     * <p>
     * Increments {@code msgBeforeCrash} and triggers {@link #crashNow()}
     * if the message count meets or exceeds the crash threshold.
     */
    private void OnCanCrashType(Serializable msg) {
        if (currentCrash == null)
            return;

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

        if (msgBeforeCrash >= currentCrash.after_n_messages_of_type) {
            crashNow();
        }
    }

    private void onWriteOK(WriteOK ignoredWriteOK) {
        if (hasCrashed)
            return;

        log("WRITEOK from coordinator: " + this.id + " " + getSender().path().name());

        CancelTimeout(writeokTimeouts.poll());
        updateSEQN++;
        Update update = this.pendingUpdates.poll();
        AppliedUpdate updateToBeApplied = new AppliedUpdate(update, epoch, updateSEQN);
        history.push(updateToBeApplied);

        debug("Applying update: " + this.id + " " + updateToBeApplied);

        debug("New history: " + this.id + " " + history);

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
     * <p>
     * After sending the WRITEOK broadcast it will begin to
     * handle the next update in the {@code coordinatorUpdateQueue}
     * if there is one.
     * </p>
     */
    private void onUpdateACK(UpdateACK updateACK) {
        if (hasCrashed) {
            return;
        }

        log("UPDATEACK <" + updateACK.updateId.replica() + "," + updateACK.updateId.id() + "> from replica "
                + getSender().path().name());

        if (currentUpdateId == null) {
            // If it's null then it must be the first update of a newly elected coordinator
            currentUpdateId = updateACK.updateId;
        }

        if (!currentUpdateId.equals(updateACK.updateId) || !acceptingUpdateAcks) {
            debug("Dropping ACK for update <" + updateACK.updateId.replica() + "," + updateACK.updateId.id() + "> from "
                    + getSender().path().name());
            return;
        }

        updateACKCount++;

        if (updateACKCount >= (replicas.size() / 2) + 1) {
            log("Write quorum reached: " + ((replicas.size() / 2) + 1) + ", broadcasting WRITEOK");

            broadcast(new WriteOK(), true);
            updateACKCount = 0;

            if (coordinatorUpdateQueue.isEmpty()) {
                coordinatorBusy = false;
                acceptingUpdateAcks = false;
            } else {
                Update update = coordinatorUpdateQueue.poll();
                currentUpdateId = update.updateId;
                broadcast(update, true);
            }
        }
    }

    private void onReadRequest(AbstractClient.ReadRequest request) {
        if (hasCrashed) {
            return;
        }

        ActorRef client = getSender();

        log("Received READ request " + request.index + " from: " + client.path().name());

        // Read immediately, return whatever this replica has
        if (request.index >= this.locations.length || request.index < 0) {
            tell(new ReadResult(false, request.index, null, this.id), client);
            debug("READ request " + request.index + " from " + client.path().name() + " - FAILED");
        } else {
            tell(new ReadResult(true, request.index, this.locations[request.index], this.id), client);
            debug("READ request " + request.index + " from " + client.path().name() + " - SUCCESS");
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
            debug("Invalid WRITE request (" + request.index + "," + request.value + ") from " + client.path().name()
                    + ", rejecting");

            tell(new AbstractClient.WriteResult(false, request.index, request.value, this.id), client);
            return;
        }

        debug("WRITE request (" + request.index + "," + request.value + ") from " + client.path().name()
                + ", adding to queue");

        writeRequests.add(new Update(new UpdateId(id, ++nextUpdateId), request, client));
        getSelf().tell(new Drain(), getSelf());
    }

    /**
     * Function that handles write requests from the {@code writeRequests} queue
     */
    private void onDrain(Drain d) {
        if (retryRequests && pendingRequests.isEmpty()) {
            retryRequests = false;
        }

        Update nextUpdate;
        if (retryRequests) {
            nextUpdate = pendingRequests.poll();
            debug("Retrying pending request with ID: " + nextUpdate.printId());
        } else {
            if (writeRequests.isEmpty()) {
                return;
            }

            nextUpdate = writeRequests.poll();
            pendingRequests.add(nextUpdate);
            debug("Pending request with ID: " + nextUpdate.printId());
        }

        debug("WRITE request (" + nextUpdate.request.index + "," + nextUpdate.request.value + ") from "
                + nextUpdate.client.path().name() + ", sending to the coordinator");

        if (id == currentCoordinator) {
            // Skip network delay for self messages
            getSelf().tell(new UpdateRequest(nextUpdate), getSelf());
        } else {
            tell(new UpdateRequest(nextUpdate), replicas.get(currentCoordinator));
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

        Update update = updateRequest.update;

        log("UPDATE REQUEST <" + update.updateId.replica() + "," + update.updateId.id() + ">");

        if (coordinatorBusy) {
            coordinatorUpdateQueue.add(update);
        } else {
            coordinatorBusy = true;
            acceptingUpdateAcks = true;
            currentUpdateId = update.updateId;
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

        log("UPDATE from coordinator: " + currentCoordinator);

        CancelTimeout(fowardTimeouts.poll());

        if (!pendingRequests.isEmpty() && update.equals(pendingRequests.peek())) {
            pendingRequests.poll();
        }

        pendingUpdates.add(update);

        if (currentCoordinator == id) {
            // Skip network delay for self message
            getSelf().tell(new UpdateACK(update.updateId), getSelf());
        } else {
            tell(new UpdateACK(update.updateId), replicas.get(currentCoordinator));
        }
        writeokTimeouts.add(getContext().system().scheduler().scheduleOnce( // ack timeout
                Duration.create(WRITEOK_TIMEOUT, TimeUnit.MILLISECONDS),
                getSelf(),
                new CoordinatorCrashed(currentCoordinator),
                getContext().system().dispatcher(),
                getSelf()));
    }

    /**
     * Handles the {@code ElectionACKTimeout} event, indicating that a replica
     * failed
     * to respond on time during the election protocol.
     * <p>
     * Upon timeout, the election message is forwarded to the node following the
     * crashed replica in the ring.
     * <p>
     * <b>Optimization:</b> If the election protocol is in its second phase (the
     * election message has
     * already completed a full ring traversal), the receiving replica removes any
     * update entries associated
     * with the crashed replica from the election message, avoiding the need to
     * restart the entire election from scratch.
     */
    private void OnElectionACKTimeout(ElectionACKTimeout electionACKTimeout) {
        int crashedReplica = electionACKTimeout.currentElection.toReplica;
        log("did NOT received ElectionACK in time by " + crashedReplica);

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

    /**
     * Handles the {@code Synchronization} message by updating the current
     * coordinator,
     * {@code epoch}, and {@code updateSEQN}.
     * <p>
     * Applies all pending updates and notifies the coordinator of any remaining
     * local pending updates.
     */
    private void onSynchronization(Synchronization synchronization) {
        log(synchronization.newCoordinator + " is the new leader");

        callbackOnCoordinatorElected(synchronization.newCoordinator);

        CancelTimeout(electionTimeout);
        epoch++;
        updateSEQN = 0;
        currentCoordinator = synchronization.newCoordinator;
        debug("Must apply these updates " + synchronization.updates);

        // All updates should be ordered already
        for (AppliedUpdate u : synchronization.updates) {
            history.push(u);
            locations[u.update.request.index] = u.update.request.value;

            callbackOnUpdateApplied(u.update.request.index, u.update.request.value);
        }

        log("Sending pending queue to the coordinator (Size: " + pendingUpdates.size() + ")");

        // Immutability is handled by the message class
        tell(new ReplicaPendingUpdates(pendingUpdates), replicas.get(currentCoordinator));
        restoreTimeout = getContext().system().scheduler().scheduleOnce(
                Duration.create(RESTORE_TIMEOUT_MS,
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
     * <p>
     * The coordinator selects the eligible updates and sends them back to the
     * replicas.
     */
    private void onReplicaPendingUpdates(ReplicaPendingUpdates updates) {
        coordinatorPendingRecovery.add(updates.pending);
        if (coordinatorPendingRecovery.size() >= (replicas.size() / 2) + 1) {
            List<Update> unique = coordinatorPendingRecovery.stream()
                    .flatMap(List::stream)
                    .distinct()
                    .toList();

            Set<UpdateId> appliedIds = history.stream()
                    .map(a -> a.update.updateId)
                    .collect(Collectors.toSet());

            List<Update> toPropagate = unique.stream()
                    .filter(u -> !appliedIds.contains(u.updateId))
                    .toList();

            log("RESTORING pending updates: " + toPropagate.size());

            broadcast(new PendingRestore(toPropagate), false);

            // Apply updates to self
            for (Update u : toPropagate) {
                AppliedUpdate a = new AppliedUpdate(u, epoch, updateSEQN++);
                history.add(a);
                locations[a.update.request.index] = a.update.request.value;

                callbackOnUpdateApplied(a.update.request.index, a.update.request.value);

                // Answer to the sender
                tell(new AbstractClient.WriteResult(true, a.update.request.index, a.update.request.value, id),
                        a.update.client);
            }
            // End of protocol
            getContext().become(createReceive());
            retryRequests = true;
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
        debug("Received restoration set from coordinator (Size: " + restore.toRestore.size() + ")");

        // Apply updates
        for (Update u : restore.toRestore) {
            AppliedUpdate a = new AppliedUpdate(u, epoch, updateSEQN++);
            history.add(a);
            locations[a.update.request.index] = a.update.request.value;

            callbackOnUpdateApplied(a.update.request.index, a.update.request.value);

            // Avoids retrying a restored update if other replicas have received it in their
            // pending queues
            pendingRequests.remove(u);
        }

        // Switch back to normal context
        getContext().become(createReceive());
        retryRequests = true;
        getSelf().tell(new Drain(), getSelf());

        pendingUpdates.clear();
        listenForHeartBeat();
    }

    private void onRestoreTimeout(RestoreTimeout timeout) {
        log("Restore timeout, coordinator crashed");
        OnCrashedCoordinator(new CoordinatorCrashed(currentCoordinator));
    }

    /**
     * Handles the {@code ElectionACK} message, indicating that a replica responded
     * on time during the election protocol and the corresponding timer can be
     * canceled.
     */
    private void OnElectionACK(ElectionACK electionACK) {
        log("received ElectionACK by " + getSender());
        CancelTimeout(electionAckExpireTimers.poll());
    }

    /**
     * Handles the {@code Election} message, serving as the core of the election
     * protocol.
     * <p>
     * This method primarily follows the paper specifications. However, once a
     * replica determines
     * that a coordinator can be elected, it schedules a timeout based on its index
     * to wait
     * for the synchronization message.
     */
    private void OnElection(Election election) {
        if (hasCrashed)
            return;
        CancelTimeout(electionTimeout);
        log("election ID: " + election.id + " received " + election);
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
                log("is the leader");
                amICoordinator = true;
                currentCoordinator = id;
                epoch++;
                updateSEQN = 0;
                sendSyncUpdates(election);
                coordinatorPendingRecovery.add(new ArrayList<>(pendingUpdates));
                replicas.keySet().retainAll(election.updates.keySet()); // update the replica set

                callbackOnCoordinatorElected(id); // the coordinator is now myself
            } else { // am not the leader to pass to the next one
                debug("Cannot be coordinator but " + newCoordinator + " should be");
                sendElection(id, election.updates, election.id);
                electionTimeout = getContext().system().scheduler().scheduleOnce( // synchronizaion message timeout
                        Duration.create((long) SYNCHRONIZAZION_TIMEOUT * getSystemNumberOfActors(),
                                TimeUnit.MILLISECONDS),
                        getSelf(),
                        new ElectionTimeout(),
                        getContext().system().dispatcher(),
                        getSelf());
            }
        }
    }

    /**
     * Handles the {@code SendHeartBeat} trigger to dispatch a {@code HeartBeat}
     * message
     * via {@code AbstractReplica::tell()}.
     */
    private void OnSendHeartBeat(SendHeartBeat sendHeartBeat) {
        broadcast(new HeartBeat(id), false);
    }

    /**
     * Handles the {@link ElectionTimeout} event. This timeout indicates one of two
     * scenarios:
     * <ul>
     * <li>The designated replica failed to send the initial election message within
     * the required time frame.</li>
     * <li>The newly elected coordinator failed to send the synchronization message
     * on time.</li>
     * </ul>
     */
    private void OnElectionTimeout(ElectionTimeout electionTimeout) {
        debug("Election timed out");
        beginElection();
    }

    /**
     * Handles the {@link ElectionStarted} message received from a replica.
     * Duplicate messages after the first two deliveries are ignored.
     * <p>
     * Removes the crashed coordinator from the replica map and initiates the
     * election process.
     *
     * <p>
     * Possible propagation strategies:
     * <ul>
     * <li>All replicas propagate the election message to the next replica in the
     * ring (simpler approach).</li>
     * <li>
     * Only a single replica initiates the election. For simplicity, the replica
     * with the smallest ID
     * sends the election message while others wait. If no message is received
     * within a specific timeout window,
     * the waiting replicas will initiate the election themselves.
     * </li>
     * </ul>
     */
    private void OnElectionStart(ElectionStarted electionStarted) {
        if (isElectionFirstPhase || hasCrashed)
            return;
        // clear timeout in case this replica did not receive a crashcoordinator message before
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(fowardTimeouts);
        CancelTimeout(writeokTimeouts);

        debug("From " + electionStarted.replicaId + " the election is started");

        isElectionFirstPhase = true;
        getContext().become(electionReceive());
        replicas.remove(currentCoordinator);

        callbackOnElectionStarted(currentCoordinator);

        int lowestKey = replicas.firstKey();
        if (lowestKey != id) {
            electionTimeout = getContext().system().scheduler().scheduleOnce( // ack timeout
                    Duration.create((long) ELECTION_TIMEOUT_MULTIPLIER * indexOfReplica(id), TimeUnit.MILLISECONDS),
                    getSelf(),
                    new ElectionTimeout(),
                    getContext().system().dispatcher(),
                    getSelf());
        } else {
            log("Begun the election");
            beginElection(); // the first must send the election directly
        }

        getContext().become(electionReceive());

        //// NOTE: this is the simple implementation where every one send the election
        //// msg
        /*
         * int nextReplica = getNextReplicaIdInRing(id);
         * ActorRef dst = replicas.get(nextReplica);
         * Election e = new Election(Map.of(id,new
         * LastUpdate(epoch,updateSEQN)),nextReplica,id);
         * 
         * debug("send election from "+id+" to " + dst + " " + e);
         * tell(e,dst);
         * electionAckExpireTimers.add(getContext().system().scheduler().scheduleOnce(
         * Duration.create(ELECTIONACK_TIMEOUT_MS, TimeUnit.MILLISECONDS),
         * getSelf(),
         * new ElectionACKTimeout(e),
         * getContext().system().dispatcher(),
         * getSelf()));
         * 
         * OR JUST SIMPLY
         * 
         * beginElection();
         */

    }

    /**
     * Handles the {@link CoordinatorCrashed} message by stopping all active
     * timeouts
     * and transitioning the context to the election state.
     * <p>
     * If an election message has not already been received, broadcasts an
     * {@link ElectionStarted} message to initiate the election process.
     */
    private void OnCrashedCoordinator(CoordinatorCrashed coordinatorCrashed) {
        CancelTimeout(heartbeatExpireTimer);
        CancelTimeout(fowardTimeouts);
        CancelTimeout(writeokTimeouts);
        if (isElectionFirstPhase)
            return;
        log("The coordinator crashed");
        broadcast(new ElectionStarted(id, currentCoordinator), true);
    }

    /**
     * Processes a received {@link HeartBeat} from the coordinator and resets
     * the timer for the next heartbeat.
     */
    private void OnHeartBeat(HeartBeat heartBeat) {
        if (hasCrashed)
            return;

        debug("Received heartbeat from coordinator " + heartBeat.currentCoordinator);

        CancelTimeout(heartbeatExpireTimer);
        listenForHeartBeat();
    }

}
