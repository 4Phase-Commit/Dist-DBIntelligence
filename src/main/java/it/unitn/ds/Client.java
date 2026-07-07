package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

import java.time.Duration;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Optional;
import java.util.Queue;

public class Client extends AbstractClient {

    private long nextTimeoutId = 0;

    private record TimeoutEnvelope(long id, Object timeout) {
    }

    // The queue stores IDs for requests under the assumption that they
    // will be processed in the same orders they are sent. IDs are used
    // to find a specific timeout to remove if it expires.
    private final Queue<Long> timeoutQueue = new ArrayDeque<>();
    private final Map<Long, Cancellable> pendingTimeouts = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
            Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
            Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica,
                Optional.ofNullable(listener)));
    }

    // =================================================================================
    // Message Sending
    // =================================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: verify whether or not this also needs to use the emulated network delay
        // from the AbstractReplica class
        setTimeout(
                new AbstractClient.ReadTimeout(getSelf(), replica, index),
                getReadTimeoutDelay());

        replica.tell(new AbstractClient.ReadRequest(index), getSelf());

        Logger.log(String.format(
                "[Client %s] requesting READ (%d) to %s",
                clientName(),
                index,
                replica.path().name()));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: verify whether or not this also needs to use the emulated network delay
        // from the AbstractReplica class
        setTimeout(
                new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
                getWriteTimeoutDelay());

        replica.tell(new AbstractClient.WriteRequest(index, value), getSelf());

        Logger.log(String.format(
                "[Client %s] requesting WRITE (%d, %d) to %s",
                clientName(),
                index,
                value,
                replica.path().name()));
    }

    private long setTimeout(Object timeoutMessage, long delay) {
        long timeoutId = nextTimeoutId++;

        Cancellable timeout = getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(delay),
                getSelf(),
                new TimeoutEnvelope(timeoutId, timeoutMessage),
                getContext().system().dispatcher(),
                getSelf());

        timeoutQueue.add(timeoutId);
        pendingTimeouts.put(timeoutId, timeout);

        return timeoutId;
    }

    // =================================================================================
    // Message handling
    // =================================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadResult.class, this::handleReadResult)
                .match(WriteResult.class, this::handleWriteResult)
                .match(TimeoutEnvelope.class, this::handleTimeout)
                .build();
    }

    public void handleReadResult(ReadResult result) {
        // Cancel scheduled timeout
        Long timeoutId = timeoutQueue.poll();

        if (timeoutId != null) {
            Cancellable timeout = pendingTimeouts.remove(timeoutId);

            if (timeout != null) {
                timeout.cancel();
            }
        }

        Logger.log(String.format(
                "[Client %s] READ complete (%s, %d, %s) from %s",
                clientName(),
                result.success,
                result.index,
                result.value,
                result.fromReplica));

        callbackOnReadResult(result);
    }

    public void handleWriteResult(WriteResult result) {
        // Cancel scheduled timeout
        Long timeoutId = timeoutQueue.poll();

        if (timeoutId != null) {
            Cancellable timeout = pendingTimeouts.remove(timeoutId);

            if (timeout != null) {
                timeout.cancel();
            }
        }

        Logger.log(String.format(
                "[Client %s] WRITE complete (%s, %d, %s) from %s",
                clientName(),
                result.success,
                result.index,
                result.value,
                result.fromReplica));

        callbackOnWriteResult(result);
    }

    public void handleTimeout(TimeoutEnvelope envelope) {
        long id = envelope.id();

        // It is assumed that there is no need to cancel
        // if the timeout expires because of scheduleOnce
        pendingTimeouts.remove(id);
        timeoutQueue.remove(id);

        Object timeoutMessage = envelope.timeout();

        if (timeoutMessage instanceof AbstractClient.ReadTimeout timeout) {
            Logger.log(String.format(
                    "[Client %s] TIMEOUT READ request to %s (%d)",
                    getSelf().path().name(),
                    timeout.replica.path().name(),
                    timeout.index));

        } else if (timeoutMessage instanceof AbstractClient.WriteTimeout timeout) {
            Logger.log(String.format(
                    "[Client %s] TIMEOUT WRITE request to %s (%d, %d)",
                    getSelf().path().name(),
                    timeout.replica.path().name(),
                    timeout.index,
                    timeout.value));
        }
    }

    // =================================================================================
    // Utils
    // =================================================================================
    private String clientName() {
        return getSelf().path().name();
    }

}
