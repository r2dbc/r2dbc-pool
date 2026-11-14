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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for a cancel that reaches the pool after the connection was handed over, while the subscriber that
 * abandoned it still has a call in flight. They run a pool of one connection against
 * {@link TransactionalStateConnection}, in both of its {@link TransactionalStateConnection.Semantics semantics}.
 */
class ConnectionPoolCancellationUnitTests {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    final List<TransactionalStateConnection> created = new CopyOnWriteArrayList<>();

    ConnectionPool pool;

    @AfterEach
    void tearDown() {
        if (this.pool != null) {
            this.pool.dispose();
        }
    }

    /**
     * What is to be tested: the shape of Spring's {@code R2dbcTransactionManager.doBegin}, which consumes the
     * connection with {@code flatMap} and has no cancel handling, cancelled while the begin is in flight, on a driver
     * that runs its calls strictly one after the other.
     * <p>
     * How will the test case be deemed successful and why: see {@link #assertCancelDuringBeginDoesNotPoison}.
     * <p>
     * Why is it important to test this test case: this is the cancel that leaves a connection with an open
     * transaction in the first place (#211, spring-framework #36344). The subscriber never closes the connection, so
     * the pool's reaction to the cancel is the only cleanup there is.
     */
    @Test
    void cancelDuringBeginShouldNotPoisonASerialDriver() {
        assertCancelDuringBeginDoesNotPoison(TransactionalStateConnection.Semantics.SERIAL);
    }

    /**
     * What is to be tested: the same as {@link #cancelDuringBeginShouldNotPoisonASerialDriver()}, on a driver that
     * decides on a rollback when it is subscribed, as r2dbc-postgresql does.
     * <p>
     * How will the test case be deemed successful and why: see {@link #assertCancelDuringBeginDoesNotPoison}.
     * <p>
     * Why is it important to test this test case: on such a driver a rollback subscribed while the begin is in flight
     * sees no transaction and does nothing, and the begin opens one after it. A pool that cleans up and returns the
     * connection hands that transaction to the next borrower; only destroying the connection avoids it.
     */
    @Test
    void cancelDuringBeginShouldNotPoisonADriverThatDecidesOnSubscription() {
        assertCancelDuringBeginDoesNotPoison(TransactionalStateConnection.Semantics.STATUS_AT_SUBSCRIPTION);
    }

    /**
     * What is to be tested: that the slot of a connection abandoned by a cancelled begin comes back into the pool.
     * <p>
     * How will the test case be deemed successful and why: after the begin has run to its end, no connection is
     * acquired, and the next acquisition gets a connection. The pool has then taken the slot back instead of leaving
     * it with a subscriber that will never close the connection.
     * <p>
     * Why is it important to test this test case: the opposite fix, ignoring every cancel after the connection was
     * handed over (#210), keeps such a slot acquired for good, and after {@code maxSize} cancels the pool is empty.
     */
    @Test
    void cancelDuringBeginShouldNotLeakTheSlot() {

        TransactionalStateConnection connection = warmup(TransactionalStateConnection.Semantics.SERIAL);
        PoolMetrics metrics = this.pool.getMetrics().get();

        cancelDuringBegin(connection).release();

        await().atMost(TIMEOUT).until(() -> connection.calls().contains("beginTransaction"));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(metrics.acquiredSize()).as("acquired").isZero());
        assertThatCode(this::transaction).doesNotThrowAnyException();
    }

    /**
     * Cancel a begin in flight, let the begin run to its end, and check that
     * <ul>
     * <li>the abandoned connection is closed and was neither rolled back nor returned: a cleanup cannot be trusted
     * while a call of the former owner still runs,</li>
     * <li>no connection is acquired,</li>
     * <li>ten transactions after it succeed, each on a connection that has no transaction open when it is handed
     * out, and none on the abandoned connection.</li>
     * </ul>
     */
    private void assertCancelDuringBeginDoesNotPoison(TransactionalStateConnection.Semantics semantics) {

        TransactionalStateConnection connection = warmup(semantics);
        PoolMetrics metrics = this.pool.getMetrics().get();

        cancelDuringBegin(connection).release();

        await().atMost(TIMEOUT).until(connection::isClosed);
        assertThat(connection.calls()).as("calls of the abandoned connection").containsExactly("beginTransaction", "close");
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(metrics.acquiredSize()).as("acquired").isZero());

        for (int i = 1; i <= 10; i++) {

            AtomicBoolean openWhenHandedOut = new AtomicBoolean();
            AtomicReference<Connection> used = new AtomicReference<>();

            assertThatCode(() -> borrow(c -> {
                TransactionalStateConnection raw = (TransactionalStateConnection) ((PooledConnection) c).unwrap();
                used.set(raw);
                openWhenHandedOut.set(raw.isTransactionOpen());
                return Mono.from(c.beginTransaction()).then(Mono.from(c.commitTransaction()));
            })).as("transaction %d", i).doesNotThrowAnyException();

            assertThat(openWhenHandedOut).as("transaction open when connection %d was handed out", i).isFalse();
            assertThat(used.get()).as("connection of transaction %d", i).isNotSameAs(connection);
        }
    }

    /**
     * Create a pool of one connection of the given semantics, create the connection and return it while it is idle.
     */
    private TransactionalStateConnection warmup(TransactionalStateConnection.Semantics semantics) {

        ConnectionFactory connectionFactory = new ConnectionFactory() {

            @Override
            public Publisher<? extends Connection> create() {
                return Mono.fromSupplier(() -> {
                    TransactionalStateConnection connection = new TransactionalStateConnection(semantics);
                    ConnectionPoolCancellationUnitTests.this.created.add(connection);
                    return connection;
                });
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "TransactionalStateConnection";
            }
        };

        this.pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory).maxSize(1).build());
        this.pool.warmup().block(TIMEOUT);

        assertThat(this.created).hasSize(1);
        return this.created.get(0);
    }

    /**
     * Acquire a connection, run {@code work} on it and close it, also when {@code work} fails, and wait for the release.
     */
    private void borrow(Function<Connection, Publisher<?>> work) {
        Mono.usingWhen(this.pool.create(), c -> Flux.from(work.apply(c)).then(), Connection::close, (c, e) -> c.close(), Connection::close).block(TIMEOUT);
    }

    /**
     * Begin and commit a transaction, close the connection, and return the driver connection that was used.
     */
    private TransactionalStateConnection transaction() {

        AtomicReference<TransactionalStateConnection> used = new AtomicReference<>();

        borrow(c -> {
            used.set((TransactionalStateConnection) ((PooledConnection) c).unwrap());
            return Mono.from(c.beginTransaction()).then(Mono.from(c.commitTransaction()));
        });

        return used.get();
    }

    /**
     * The shape of Spring's {@code R2dbcTransactionManager.doBegin}: the connection is consumed with {@code flatMap}
     * and nothing handles a cancel. The queued part of the begin is held; the subscriber is disposed once the begin is
     * next in the driver's queue, and the begin goes on when the returned hold is released.
     */
    private TransactionalStateConnection.Hold cancelDuringBegin(TransactionalStateConnection connection) {

        TransactionalStateConnection.Hold begin = connection.hold("beginTransaction");

        Disposable subscription = this.pool.create().flatMap(c -> Mono.from(c.beginTransaction(IsolationLevel.READ_COMMITTED))).subscribe();
        begin.awaitReached(TIMEOUT);

        subscription.dispose();
        return begin;
    }

}
