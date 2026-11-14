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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for connections that go back into the pool with an open transaction or without the auto-commit mode
 * they were created with. They run a pool of one connection against {@link TransactionalStateConnection}.
 */
class ConnectionPoolBaselineUnitTests {

    static final Duration TIMEOUT = Duration.ofSeconds(5);

    final List<TransactionalStateConnection> created = new CopyOnWriteArrayList<>();

    ConnectionPool pool;

    @BeforeEach
    void setUp() {

        this.pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory(TransactionalStateConnection::new)).maxSize(1).build());
    }

    private ConnectionFactory connectionFactory(Supplier<TransactionalStateConnection> connections) {

        return new ConnectionFactory() {

            @Override
            public Publisher<? extends Connection> create() {
                return Mono.fromSupplier(() -> {
                    TransactionalStateConnection connection = connections.get();
                    ConnectionPoolBaselineUnitTests.this.created.add(connection);
                    return connection;
                });
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "TransactionalStateConnection";
            }
        };
    }

    @AfterEach
    void tearDown() {
        this.pool.dispose();
    }

    /**
     * What is to be tested: that a connection which lies idle in manual-commit mode with an open transaction, as a
     * failure outside the pool's view leaves it, is repaired by the release after the first failed begin.
     * <p>
     * How will the test case be deemed successful and why: the first transaction fails with {@code ORA-01453}, which
     * shows that the poisoned state is real; the ten transactions after it succeed, and the connection is in
     * auto-commit mode after each release. Then the release of the failed borrower has rolled back and restored the
     * auto-commit mode the connection was created with.
     * <p>
     * Why is it important to test this test case: without the repair every begin on that connection fails until
     * {@code maxLifeTime} removes it, the loop reported in #233. One failed request is acceptable, a pool slot that
     * fails every request is not.
     */
    @Test
    void poisonedConnectionRecoversAfterOneFailedBegin() {

        TransactionalStateConnection connection = warmup();
        connection.forceState(false, true);

        assertThatThrownBy(this::transaction).hasMessageContaining("ORA-01453");

        for (int i = 1; i <= 10; i++) {
            AtomicReference<TransactionalStateConnection> used = new AtomicReference<>();
            assertThatCode(() -> used.set(transaction())).as("transaction %d after the failed begin", i).doesNotThrowAnyException();
            assertThat(used.get().isAutoCommit()).as("auto-commit after release %d", i).isTrue();
        }
    }

    /**
     * What is to be tested: a borrower whose commit fails, whose restore of auto-commit fails as well, and which does
     * not roll back by itself before it closes the connection.
     * <p>
     * How will the test case be deemed successful and why: both armed failures happened; the release rolled back
     * before it switched auto-commit on again; the next transaction succeeds. The order matters, because switching
     * auto-commit on commits an open transaction: only a rollback first discards the work of the failed borrower
     * instead of committing it. Then no transaction is left open, although the borrower did nothing to clean up.
     * <p>
     * Why is it important to test this test case: the error paths of a borrower are where the pool's cleanup is the
     * last line of defence. It combines two gaps of #233: a failed commit that clears the flag, and a mode that is
     * never restored.
     */
    @Test
    void failedCommitAndRestoreShouldNotPoison() {

        TransactionalStateConnection connection = warmup();
        connection.failNext("commitTransaction").failNext("setAutoCommit");

        // A borrower that does not roll back by itself.
        borrow(c -> Mono.from(c.beginTransaction())
            .thenMany(c.createStatement("INSERT INTO t VALUES (1)").execute())
            .then(Mono.from(c.commitTransaction()))
            .onErrorResume(commitFailed -> Mono.from(c.setAutoCommit(true)))
            .onErrorResume(restoreFailed -> Mono.empty()));

        assertThat(connection.calls()).contains("commitTransaction:failed", "setAutoCommit(true):failed");
        assertThat(connection.calls()).as("calls up to the end of the release")
            .containsSubsequence("commitTransaction:failed", "setAutoCommit(true):failed", "rollbackTransaction", "setAutoCommit(true)");

        assertThatCode(this::transaction).doesNotThrowAnyException();
    }

    /**
     * What is to be tested: a borrower that switches auto-commit off on the driver connection and runs a DML
     * statement, which opens a transaction the pool never saw begin, and then closes the connection.
     * <p>
     * How will the test case be deemed successful and why: the release rolled back before it switched auto-commit on
     * again, and the next transaction succeeds. The release has then noticed that the connection deviates from its
     * auto-commit baseline, and it discarded the borrower's uncommitted work instead of committing it, which switching
     * auto-commit on alone would do.
     * <p>
     * Why is it important to test this test case: the pool's own flag only sees {@code beginTransaction()} through
     * {@code PooledConnection}. Code that uses {@code unwrap()} or {@code setAutoCommit(false)} bypasses it, and only
     * the comparison with the baseline catches it.
     */
    @Test
    void implicitTransactionShouldNotPoison() {

        TransactionalStateConnection connection = warmup();

        borrow(c -> {
            Connection raw = ((PooledConnection) c).unwrap();
            return Mono.from(raw.setAutoCommit(false)).thenMany(raw.createStatement("INSERT INTO t VALUES (1)").execute()).then();
        });

        assertThat(connection.calls()).as("calls up to the end of the release")
            .containsSubsequence("setAutoCommit(false)", "execute(INSERT INTO t VALUES (1))", "rollbackTransaction", "setAutoCommit(true)");

        assertThatCode(this::transaction).doesNotThrowAnyException();
    }

    /**
     * What is to be tested: Spring's order of calls when only the restore of auto-commit fails: begin, a commit that
     * succeeds, {@code setAutoCommit(true)} that fails, close.
     * <p>
     * How will the test case be deemed successful and why: the armed failure happened; the connection is in
     * auto-commit mode after the release; a statement of the next borrower outside a transaction leaves no transaction
     * open, and the begin after it succeeds. Then the pool restored the auto-commit mode the connection was created
     * with, although the borrower failed to.
     * <p>
     * Why is it important to test this test case: in manual-commit mode a plain statement of the next borrower opens a
     * transaction that nobody commits, and the following begin fails on it. The restore by the borrower is not enough
     * when it fails.
     */
    @Test
    void failedRestoreShouldNotLeaveManualCommitMode() {

        TransactionalStateConnection connection = warmup();
        connection.failNext("setAutoCommit");

        // Spring's order when only the restore fails.
        borrow(c -> Mono.from(c.beginTransaction())
            .then(Mono.from(c.commitTransaction()))
            .then(Mono.from(c.setAutoCommit(true)))
            .onErrorResume(restoreFailed -> Mono.empty()));

        assertThat(connection.calls()).contains("commitTransaction", "setAutoCommit(true):failed");
        assertThat(connection.isAutoCommit()).as("auto-commit after the release").isTrue();

        AtomicBoolean openAfterStatement = new AtomicBoolean();
        borrow(c -> Flux.from(c.createStatement("INSERT INTO t VALUES (2)").execute())
            .then(Mono.fromRunnable(() -> openAfterStatement.set(connection.isTransactionOpen())))
            .then(Mono.from(c.beginTransaction()))
            .then(Mono.from(c.commitTransaction())));

        assertThat(openAfterStatement).as("transaction open after a statement outside a transaction").isFalse();
    }

    /**
     * What is to be tested: a pool whose connections are created in manual-commit mode, and a borrower that runs a DML
     * statement and closes the connection without committing or rolling back.
     * <p>
     * How will the test case be deemed successful and why: the release rolled back after the statement, no
     * transaction is open afterwards, and the connection is still in manual-commit mode. The release discarded the
     * borrower's uncommitted work and kept the mode the connection was created with.
     * <p>
     * Why is it important to test this test case: in manual-commit mode the statement opens a transaction implicitly,
     * which the pool never sees begin. Without the rollback the next borrower would work inside it and could commit it.
     */
    @Test
    void implicitTransactionOnManualCommitConnectionShouldNotPoison() {

        this.pool.dispose();
        this.created.clear();
        this.pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory(() -> {
            TransactionalStateConnection connection = new TransactionalStateConnection();
            connection.forceState(false, false);
            return connection;
        })).maxSize(1).build());

        TransactionalStateConnection connection = warmup();

        borrow(c -> c.createStatement("INSERT INTO t VALUES (1)").execute());

        assertThat(connection.calls()).as("calls up to the end of the release")
            .containsSubsequence("execute(INSERT INTO t VALUES (1))", "rollbackTransaction");
        assertThat(connection.calls()).doesNotContain("setAutoCommit(true)", "setAutoCommit(false)");
        assertThat(connection.isTransactionOpen()).as("transaction open after the release").isFalse();
        assertThat(connection.isAutoCommit()).as("auto-commit after the release").isFalse();
    }

    /**
     * What is to be tested: a driver that reports manual-commit mode while a transaction is open and decides on a
     * rollback when it is subscribed, as r2dbc-postgresql does, and a borrower that begins a transaction on the driver
     * connection and closes without committing.
     * <p>
     * How will the test case be deemed successful and why: the release rolled back, no transaction is open afterwards,
     * and the connection reports auto-commit mode without a {@code setAutoCommit(…)} call. The pool saw no begin, but
     * the open transaction showed as manual-commit mode, and the rollback ended it.
     * <p>
     * Why is it important to test this test case: on such a driver the mode is the only sign of a transaction the pool
     * did not see begin, and switching auto-commit on would commit it instead of discarding it.
     */
    @Test
    void transactionBegunOnTheDriverConnectionShouldNotPoisonADriverThatDecidesOnSubscription() {

        this.pool.dispose();
        this.created.clear();
        this.pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory(() -> new TransactionalStateConnection(TransactionalStateConnection.Semantics.STATUS_AT_SUBSCRIPTION)))
            .maxSize(1).build());

        TransactionalStateConnection connection = warmup();

        borrow(c -> Mono.from(((PooledConnection) c).unwrap().beginTransaction()).thenMany(c.createStatement("INSERT INTO t VALUES (1)").execute()));

        assertThat(connection.calls()).as("calls up to the end of the release")
            .containsSubsequence("beginTransaction", "execute(INSERT INTO t VALUES (1))", "rollbackTransaction");
        assertThat(connection.calls()).doesNotContain("commitTransaction");
        assertThat(connection.isTransactionOpen()).as("transaction open after the release").isFalse();
        assertThat(connection.isAutoCommit()).as("auto-commit after the release").isTrue();
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

}
