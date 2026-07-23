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
    private Map<ReadTimeout, Queue<Cancellable>> readTimeouts = new HashMap<>();
    private Map<WriteTimeout, Queue<Cancellable>> writeTimeouts = new HashMap<>();

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
    // Messages for controlling the client
    // =================================================================================
    public static class SendReadMessage {
        public final ActorRef replica;
        public final int key;

        public SendReadMessage(ActorRef replica, int key) {
            this.replica = replica;
            this.key = key;
        }
    }

    public static class SendWriteMessage {
        public final ActorRef replica;
        public final int key;
        public final int value;

        public SendWriteMessage(ActorRef replica, int key, int value) {
            this.replica = replica;
            this.key = key;
            this.value = value;
        }
    }

    // =================================================================================
    // Message Sending
    // =================================================================================

    @Override
    public void sendRead(ActorRef replica, int index) {
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
        setTimeout(
                new AbstractClient.WriteTimeout(getSelf(), replica, index, value),
                getWriteTimeoutDelay());

        replica.tell(new AbstractClient.WriteRequest(index, value, replica), getSelf());

        Logger.log(String.format(
                "[Client %s] requesting WRITE (%d, %d) to %s",
                clientName(),
                index,
                value,
                replica.path().name()));
    }

    private void setTimeout(Object timeoutMessage, long delay) {
        Cancellable timeout = getContext().system().scheduler().scheduleOnce(
                Duration.ofMillis(delay),
                getSelf(),
                timeoutMessage,
                getContext().system().dispatcher(),
                getSelf());

        if (timeoutMessage instanceof ReadTimeout) {
            ReadTimeout rTimeout = (ReadTimeout) timeoutMessage;
            readTimeouts
                    .computeIfAbsent(rTimeout, k -> new ArrayDeque<>())
                    .add(timeout);
        } else if (timeoutMessage instanceof WriteTimeout) {
            WriteTimeout wTimeout = (WriteTimeout) timeoutMessage;
            writeTimeouts
                    .computeIfAbsent(wTimeout, k -> new ArrayDeque<>())
                    .add(timeout);
        } else {
            throw new IllegalArgumentException("Unknown timeout type: " + timeout.getClass());
        }
    }

    private void cancelTimeout(ReadTimeout timeout) {
        // Logger.debug("Read Timeout Queue: " +
        // readTimeouts.values().stream().mapToInt(Queue::size).sum());
        Queue<Cancellable> queue = readTimeouts.get(timeout);

        if (queue != null) {
            Cancellable c = queue.poll();
            if (c != null) {
                c.cancel();
            }

            if (queue.isEmpty()) {
                readTimeouts.remove(timeout);
            }
        }
    }

    private void cancelTimeout(WriteTimeout timeout) {
        // Logger.debug("Write Timeout Queue: " +
        // writeTimeouts.values().stream().mapToInt(Queue::size).sum());
        Queue<Cancellable> queue = writeTimeouts.get(timeout);

        if (queue != null) {
            Cancellable c = queue.poll();
            if (c != null) {
                c.cancel();
            }

            if (queue.isEmpty()) {
                writeTimeouts.remove(timeout);
            }
        }
    }

    // =================================================================================
    // Message handling
    // =================================================================================

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(SendReadMessage.class, this::handleSendReadCommand)
                .match(SendWriteMessage.class, this::handleSendWriteCommand)
                .match(ReadResult.class, this::handleReadResult)
                .match(WriteResult.class, this::handleWriteResult)
                .match(ReadTimeout.class, this::handleTimeout)
                .match(WriteTimeout.class, this::handleTimeout)
                .build();
    }

    public void handleSendReadCommand(SendReadMessage msg) {
        Logger.log(String.format(
                "[Client %s] Received READ command message for index (%d)",
                clientName(),
                msg.key));

        sendRead(msg.replica, msg.key);
    }

    public void handleSendWriteCommand(SendWriteMessage msg) {
        Logger.log(String.format(
                "[Client %s] Received WRITE command message for (%d, %d)",
                clientName(),
                msg.key,
                msg.value));

        sendWrite(msg.replica, msg.key, msg.value);
    }

    public void handleReadResult(ReadResult result) {
        // Cancel scheduled timeout
        ReadTimeout timeout = new ReadTimeout(getSelf(), getSender(), result.index);
        cancelTimeout(timeout);

        // Duplicate log with the callback
        // Logger.log(String.format(
        // "[Client %s] READ complete (%s, %d, %s) from %s",
        // clientName(),
        // result.success,
        // result.index,
        // result.value,
        // result.fromReplica));

        callbackOnReadResult(result);
    }

    public void handleWriteResult(WriteResult result) {
        // Cancel scheduled timeout
        WriteTimeout timeout = new WriteTimeout(getSelf(), getSender(), result.index, result.value);
        cancelTimeout(timeout);

        // Duplicate log with the callback
        // Logger.log(String.format(
        // "[Client %s] WRITE complete (%s, %d, %s) from %s",
        // clientName(),
        // result.success,
        // result.index,
        // result.value,
        // result.fromReplica));

        callbackOnWriteResult(result);
    }

    public void handleTimeout(Object timeout) {
        if (timeout instanceof ReadTimeout) {
            ReadTimeout rTimeout = (ReadTimeout) timeout;
            cancelTimeout(rTimeout);
            callbackOnReadTimeout(rTimeout);

            Logger.log(String.format(
                    "[Client %s] TIMEOUT READ request to %s (%d)",
                    clientName(),
                    rTimeout.replica.path().name(),
                    rTimeout.index));

        } else if (timeout instanceof WriteTimeout) {
            WriteTimeout wTimeout = (WriteTimeout) timeout;
            cancelTimeout(wTimeout);
            callbackOnWriteTimeout(wTimeout);

            Logger.log(String.format(
                    "[Client %s] TIMEOUT WRITE request to %s (%d, %d)",
                    clientName(),
                    wTimeout.replica.path().name(),
                    wTimeout.index,
                    wTimeout.value));

        } else {
            Logger.log(String.format(
                    "[Client %s] Received an invalid timeout message",
                    getSelf().path().name()));
        }
    }

    // =================================================================================
    // Utils
    // =================================================================================
    private String clientName() {
        return getSelf().path().name();
    }

}
