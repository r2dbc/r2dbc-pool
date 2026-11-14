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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * Regression tests for a cancel that reaches the pool after the connection was handed over, while the subscriber that
 * abandoned it still has a call in flight. They run a pool of one connection against
 * {@link TransactionalStateConnection}.
 */
class ConnectionPoolCancellationUnitTests {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    final List<TransactionalStateConnection> created = new CopyOnWriteArrayList<>();

    ConnectionPool pool;

    @BeforeEach
    void setUp() {

        ConnectionFactory connectionFactory = new ConnectionFactory() {

            @Override
            public Publisher<? extends Connection> create() {
                return Mono.fromSupplier(() -> {
                    TransactionalStateConnection connection = new TransactionalStateConnection();
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
    }

    @AfterEach
    void tearDown() {
        this.pool.dispose();
    }

    /**
     * What is to be tested: the shape of Spring's {@code R2dbcTransactionManager.doBegin}, which consumes the
     * connection with {@code flatMap} and has no cancel handling, cancelled while the begin is in flight.
     * <p>
     * How will the test case be deemed successful and why: the connection is idle again only after a rollback ran;
     * once the begin has run to its end no transaction is open; ten transactions after it succeed. Then the pool has
     * taken the abandoned connection back through its release path, and the rollback ran behind the begin that was
     * still in flight.
     * <p>
     * Why is it important to test this test case: this is the cancel that leaves a connection with an open
     * transaction in the first place (#211, spring-framework #36344). The subscriber never closes the connection, so
     * the pool's reaction to the cancel is the only cleanup there is.
     */
    @Test
    void cancelDuringBeginShouldNotPoison() {

        TransactionalStateConnection connection = warmup();
        PoolMetrics metrics = this.pool.getMetrics().get();

        cancelDuringBegin(connection).release();

        await().atMost(TIMEOUT).until(() -> metrics.idleSize() == 1);
        assertThat(connection.calls()).as("calls before the connection was idle again").containsSubsequence("beginTransaction", "rollbackTransaction");
        assertThat(connection.isTransactionOpen()).as("transaction open after the cancelled begin has run").isFalse();

        for (int i = 1; i <= 10; i++) {
            assertThatCode(this::transaction).as("transaction %d", i).doesNotThrowAnyException();
        }
    }

    /**
     * What is to be tested: that the slot of a connection abandoned by a cancelled begin comes back into the pool.
     * <p>
     * How will the test case be deemed successful and why: after the begin has run to its end, no connection is
     * acquired and one is idle. The pool has then taken the connection back instead of leaving it with a subscriber
     * that will never close it.
     * <p>
     * Why is it important to test this test case: the opposite fix, ignoring every cancel after the connection was
     * handed over (#210), keeps such a slot acquired for good, and after {@code maxSize} cancels the pool is empty.
     * The test passes today and guards against that regression.
     */
    @Test
    void cancelDuringBeginShouldNotLeakTheSlot() {

        TransactionalStateConnection connection = warmup();
        PoolMetrics metrics = this.pool.getMetrics().get();

        cancelDuringBegin(connection).release();

        await().atMost(TIMEOUT).until(() -> connection.calls().contains("beginTransaction"));
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(metrics.acquiredSize()).as("acquired").isZero();
            assertThat(metrics.idleSize()).as("idle").isOne();
        });
    }

    /**
     * Create the pool's only connection and return it while it is idle.
     */
    private TransactionalStateConnection warmup() {

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
