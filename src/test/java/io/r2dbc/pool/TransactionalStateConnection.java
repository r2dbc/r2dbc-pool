/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.pool;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A stateful fake {@link Connection} that models the transactional behaviour of a driver. In both {@link Semantics}
 * it
 * <ul>
 * <li>runs every call after the previous one has finished (one queue per connection),</li>
 * <li>runs a call that was subscribed to its end even if its subscriber cancels.</li>
 * </ul>
 * Queries and validations never open a transaction. Calls can be delayed, held until a test lets them go on, and
 * armed to fail or to hang once.
 */
final class TransactionalStateConnection implements Connection {

    enum Behaviour {
        FAIL, HANG
    }

    /**
     * How the driver decides on transactional calls.
     */
    enum Semantics {

        /**
         * Like oracle-r2dbc: a begin takes two steps, auto-commit is switched off on subscription, then a queued step
         * fails if a transaction is already open. Switching auto-commit on commits an open transaction, and a DML
         * statement in manual-commit mode opens a transaction implicitly.
         */
        SERIAL,

        /**
         * Like r2dbc-postgresql: a begin, commit or rollback reads the transaction status when it is subscribed and
         * does nothing if the status does not call for it; the status changes only when the queued call has run.
         * {@code isAutoCommit()} reports {@code false} while a transaction is open, and {@code setAutoCommit(…)}
         * begins or commits one.
         */
        STATUS_AT_SUBSCRIPTION
    }

    /**
     * A call of one operation held before its queued part runs.
     */
    static final class Hold {

        private final CompletableFuture<Void> reached = new CompletableFuture<>();

        private final CompletableFuture<Void> released = new CompletableFuture<>();

        /**
         * Wait until the held call is next in the queue, all calls before it having run.
         */
        void awaitReached(Duration timeout) {
            try {
                this.reached.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (ExecutionException | TimeoutException e) {
                throw new IllegalStateException("The held call was not reached", e);
            }
        }

        /**
         * Let the held call run.
         */
        void release() {
            this.released.complete(null);
        }
    }

    private final Semantics semantics;

    private final Object lock = new Object();

    private final List<String> calls = new CopyOnWriteArrayList<>();

    private final Map<String, Duration> delays = new ConcurrentHashMap<>();

    private final Map<String, ConcurrentLinkedQueue<Behaviour>> armed = new ConcurrentHashMap<>();

    private final Map<String, ConcurrentLinkedQueue<Hold>> holds = new ConcurrentHashMap<>();

    private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

    private volatile boolean autoCommit = true;

    private volatile boolean transactionOpen = false;

    private volatile boolean closed = false;

    private volatile boolean closedWithOpenTransaction = false;

    TransactionalStateConnection() {
        this(Semantics.SERIAL);
    }

    TransactionalStateConnection(Semantics semantics) {
        this.semantics = semantics;
    }

    /**
     * Delay the queued part of every following call of {@code operation} by {@code delay}.
     */
    TransactionalStateConnection delay(String operation, Duration delay) {
        this.delays.put(operation, delay);
        return this;
    }

    /**
     * Hold the queued part of the next call of {@code operation} until {@link Hold#release()}. Calls behind it wait in
     * the queue.
     */
    Hold hold(String operation) {
        Hold hold = new Hold();
        this.holds.computeIfAbsent(operation, it -> new ConcurrentLinkedQueue<>()).add(hold);
        return hold;
    }

    /**
     * Let the next call of {@code operation} fail without changing any state.
     */
    TransactionalStateConnection failNext(String operation) {
        this.armed.computeIfAbsent(operation, it -> new ConcurrentLinkedQueue<>()).add(Behaviour.FAIL);
        return this;
    }

    /**
     * Let the next call of {@code operation} never complete. Like the driver it models, the connection then runs no
     * further call.
     */
    TransactionalStateConnection hangNext(String operation) {
        this.armed.computeIfAbsent(operation, it -> new ConcurrentLinkedQueue<>()).add(Behaviour.HANG);
        return this;
    }

    /**
     * Put the connection into a state directly, as a failure outside the pool's view would leave it.
     */
    void forceState(boolean autoCommit, boolean transactionOpen) {
        synchronized (this.lock) {
            this.autoCommit = autoCommit;
            this.transactionOpen = transactionOpen;
        }
    }

    boolean isTransactionOpen() {
        return this.transactionOpen;
    }

    boolean isClosed() {
        return this.closed;
    }

    boolean wasClosedWithOpenTransaction() {
        return this.closedWithOpenTransaction;
    }

    /**
     * The calls in the order they ran to their end, a failed call with the suffix {@code :failed}.
     */
    List<String> calls() {
        return new ArrayList<>(this.calls);
    }

    @Override
    public Publisher<Void> beginTransaction() {
        return begin();
    }

    @Override
    public Publisher<Void> beginTransaction(TransactionDefinition definition) {
        return begin();
    }

    private Mono<Void> begin() {
        return Mono.defer(() -> {

            if (this.semantics == Semantics.STATUS_AT_SUBSCRIPTION) {
                return unlessStatusSays("beginTransaction", this.transactionOpen, () -> this.transactionOpen = true);
            }

            // Step 1 runs on subscription.
            synchronized (this.lock) {
                this.autoCommit = false;
            }

            // Step 2 is queued.
            return enqueue("beginTransaction", () -> {
                if (this.transactionOpen) {
                    throw new R2dbcNonTransientResourceException("ORA-01453: SET TRANSACTION must be first statement of transaction");
                }
                this.transactionOpen = true;
            });
        });
    }

    @Override
    public Publisher<Void> commitTransaction() {

        if (this.semantics == Semantics.STATUS_AT_SUBSCRIPTION) {
            return Mono.defer(() -> unlessStatusSays("commitTransaction", !this.transactionOpen, () -> this.transactionOpen = false));
        }

        return enqueue("commitTransaction", () -> {
            if (!this.autoCommit) {
                this.transactionOpen = false;
            }
        });
    }

    @Override
    public Publisher<Void> rollbackTransaction() {

        if (this.semantics == Semantics.STATUS_AT_SUBSCRIPTION) {
            return Mono.defer(() -> unlessStatusSays("rollbackTransaction", !this.transactionOpen, () -> this.transactionOpen = false));
        }

        return enqueue("rollbackTransaction", () -> {
            if (!this.autoCommit) {
                this.transactionOpen = false;
            }
        });
    }

    @Override
    public Publisher<Void> setAutoCommit(boolean autoCommit) {

        if (this.semantics == Semantics.STATUS_AT_SUBSCRIPTION) {
            return Mono.defer(() -> {
                if (autoCommit != isAutoCommit()) {
                    return autoCommit ? Mono.from(commitTransaction()) : begin();
                }
                return Mono.empty();
            });
        }

        return enqueue("setAutoCommit", "setAutoCommit(" + autoCommit + ")", () -> {
            if (autoCommit && !this.autoCommit) {
                // Switching auto-commit on commits an open transaction first.
                this.transactionOpen = false;
            }
            this.autoCommit = autoCommit;
        });
    }

    @Override
    public boolean isAutoCommit() {
        return this.semantics == Semantics.STATUS_AT_SUBSCRIPTION ? !this.transactionOpen : this.autoCommit;
    }

    @Override
    public Statement createStatement(String sql) {
        return new FakeStatement(sql);
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth depth) {

        if (depth == ValidationDepth.LOCAL) {
            return Mono.fromSupplier(() -> !this.closed);
        }

        return enqueue("validate", () -> {
        }).then(Mono.fromSupplier(() -> !this.closed));
    }

    @Override
    public Publisher<Void> close() {
        return enqueue("close", () -> {
            this.closedWithOpenTransaction = this.transactionOpen;
            this.closed = true;
        });
    }

    @Override
    public Batch createBatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Publisher<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Publisher<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Publisher<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectionMetadata getMetadata() {
        return new ConnectionMetadata() {

            @Override
            public String getDatabaseProductName() {
                return "TransactionalStateConnection";
            }

            @Override
            public String getDatabaseVersion() {
                return "1";
            }
        };
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        return IsolationLevel.READ_COMMITTED;
    }

    @Override
    public Publisher<Void> setLockWaitTimeout(Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setStatementTimeout(Duration timeout) {
        return Mono.empty();
    }

    @Override
    public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        return Mono.empty();
    }

    @Override
    public String toString() {
        return "TransactionalStateConnection[autoCommit=" + this.autoCommit + ", transactionOpen=" + this.transactionOpen + ", closed=" + this.closed + "]";
    }

    private Mono<Void> enqueue(String operation, Runnable action) {
        return enqueue(operation, operation, action);
    }

    /**
     * Queue {@code action} unless the status read now, on subscription, says that there is nothing to do; then record
     * the call as skipped.
     */
    private Mono<Void> unlessStatusSays(String operation, boolean nothingToDo, Runnable action) {

        if (nothingToDo) {
            this.calls.add(operation + ":skipped");
            return Mono.empty();
        }

        return enqueue(operation, action);
    }

    /**
     * Queue {@code action} behind the previous call. The returned {@link Mono} completes when the action has run;
     * cancelling it does not stop the action.
     */
    private Mono<Void> enqueue(String operation, String label, Runnable action) {
        return Mono.defer(() -> {

            CompletableFuture<Void> done = new CompletableFuture<>();
            CompletableFuture<Void> previous;

            synchronized (this.lock) {
                previous = this.tail;
                this.tail = done;
            }

            ConcurrentLinkedQueue<Hold> holds = this.holds.get(operation);
            Hold hold = holds != null ? holds.poll() : null;

            previous.whenComplete((ignore, previousError) -> {

                Runnable step = () -> run(operation, label, action, done);
                Duration delay = this.delays.getOrDefault(operation, Duration.ZERO);

                if (hold != null) {
                    hold.released.thenRun(step);
                    hold.reached.complete(null);
                } else if (delay.isZero()) {
                    step.run();
                } else {
                    Schedulers.parallel().schedule(step, delay.toMillis(), TimeUnit.MILLISECONDS);
                }
            });

            // A future of its own, so that a cancel of the subscriber does not reach the queued call.
            CompletableFuture<Void> result = new CompletableFuture<>();
            done.whenComplete((ignore, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(null);
                }
            });

            return Mono.fromFuture(result);
        });
    }

    private void run(String operation, String label, Runnable action, CompletableFuture<Void> done) {

        ConcurrentLinkedQueue<Behaviour> behaviours = this.armed.get(operation);
        Behaviour behaviour = behaviours != null ? behaviours.poll() : null;

        if (behaviour == Behaviour.HANG) {
            this.calls.add(label + ":hanging");
            return;
        }

        if (behaviour == Behaviour.FAIL) {
            this.calls.add(label + ":failed");
            done.completeExceptionally(new R2dbcNonTransientResourceException(label + " failed"));
            return;
        }

        try {
            synchronized (this.lock) {
                action.run();
            }
            this.calls.add(label);
            done.complete(null);
        } catch (RuntimeException e) {
            this.calls.add(label + ":failed");
            done.completeExceptionally(e);
        }
    }

    final class FakeStatement implements Statement {

        private final String sql;

        FakeStatement(String sql) {
            this.sql = sql;
        }

        @Override
        public Statement add() {
            return this;
        }

        @Override
        public Statement bind(int index, Object value) {
            return this;
        }

        @Override
        public Statement bind(String name, Object value) {
            return this;
        }

        @Override
        public Statement bindNull(int index, Class<?> type) {
            return this;
        }

        @Override
        public Statement bindNull(String name, Class<?> type) {
            return this;
        }

        @Override
        public Publisher<? extends Result> execute() {

            boolean dml = !this.sql.trim().toUpperCase().startsWith("SELECT");

            return enqueue("execute", "execute(" + this.sql + ")", () -> {
                if (dml && TransactionalStateConnection.this.semantics == Semantics.SERIAL && !TransactionalStateConnection.this.autoCommit) {
                    TransactionalStateConnection.this.transactionOpen = true;
                }
            }).thenMany(Flux.<Result>empty());
        }
    }
}
