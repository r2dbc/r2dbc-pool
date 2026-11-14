/*
 * Copyright 2019-2021 the original author or authors.
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
import io.r2dbc.spi.Lifecycle;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import io.r2dbc.spi.Wrapped;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.pool.PooledRef;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Function;

/**
 * Pooled {@link Connection} implementation. Performs a cleanup on {@link #close()}: rolls back a transaction that was
 * left open and restores the auto-commit mode the connection had when it was created. A failure of the cleanup, or of a
 * {@code preRelease} function, invalidates the connection instead of returning it to the pool.
 * <p>
 *
 * @author Mark Paluch
 */
final class PooledConnection implements Connection, Wrapped<Connection> {

    private static final Logger logger = Loggers.getLogger(PooledConnection.class);

    private final PooledRef<Connection> ref;

    private final Connection connection;

    private final Mono<Void> release;

    @Nullable
    private final Boolean autoCommitBaseline;

    private volatile boolean closed = false;

    private volatile boolean inTransaction = false;

    PooledConnection(PooledRef<Connection> ref) {
        this(ref, null);
    }

    PooledConnection(PooledRef<Connection> ref, @Nullable Function<? super Connection, ? extends Publisher<Void>> preRelease) {
        this(ref, preRelease, null);
    }

    /**
     * @param autoCommitBaseline the auto-commit mode the connection had when it was created, restored on release;
     *                           {@code null} if unknown, in which case only an unfinished transaction is rolled back
     */
    PooledConnection(PooledRef<Connection> ref, @Nullable Function<? super Connection, ? extends Publisher<Void>> preRelease, @Nullable Boolean autoCommitBaseline) {
        this.ref = ref;
        this.connection = ref.poolable();
        this.autoCommitBaseline = autoCommitBaseline;
        this.release = Mono.defer(() -> {
            return Validation.validate(this, ValidationDepth.LOCAL).then(Mono.defer(() -> {

                Mono<Void> cleanup = null;

                if (preRelease != null) {
                    cleanup = Mono.from(preRelease.apply(this.connection));
                }

                // Deferred, so that it sees the state the preRelease function left behind.
                Mono<Void> reset = resetTransactionState();
                cleanup = cleanup == null ? reset : cleanup.then(reset);

                if (this.connection instanceof Lifecycle) {

                    Mono<Void> lifecyclePreRelease = Mono.defer(() -> Mono.from(((Lifecycle) this.connection).preRelease()));
                    cleanup = cleanup == null ? lifecyclePreRelease : cleanup.then(lifecyclePreRelease);
                }

                return cleanup.doOnSubscribe(ignore -> this.closed = true).then(this.ref.release());
            })).onErrorResume(throwable -> handleCleanupError(throwable, ref)).doOnSubscribe(subscription -> logger.debug("Releasing connection"));
        });
    }

    /**
     * Roll back a transaction that is left open and restore the auto-commit mode the connection was created with.
     * <p>
     * A rollback runs if a transaction was begun and not committed, or if the connection is in manual-commit mode
     * although it was created in auto-commit mode: manual-commit mode may hide a transaction the pool did not see
     * begin. The auto-commit mode is restored only after the rollback, because switching auto-commit on commits an
     * open transaction. An error stops the release, and the connection is invalidated.
     */
    private Mono<Void> resetTransactionState() {
        return Mono.defer(() -> {

            Boolean baseline = this.autoCommitBaseline;
            boolean deviates = baseline != null && this.connection.isAutoCommit() != baseline;

            // Deviating from an auto-commit baseline means manual-commit mode.
            boolean rollback = this.inTransaction || (deviates && baseline);

            Mono<Void> reset = rollback ? rollbackTransaction() : Mono.empty();

            if (deviates) {
                // The driver connection: this connection is already marked closed.
                reset = reset.then(Mono.defer(() -> Mono.from(this.connection.setAutoCommit(baseline))));
            }

            return reset;
        });
    }

    private static Mono<Void> handleCleanupError(Throwable throwable, PooledRef<Connection> ref) {

        logger.debug("Release failed", throwable);
        return ref.invalidate();
    }

    @Override
    public Mono<Void> beginTransaction() {
        assertNotClosed();
        return Mono.from(this.connection.beginTransaction()).doOnSubscribe(ignore -> this.inTransaction = true).doOnError(e -> this.inTransaction = false);
    }

    @Override
    public Mono<Void> beginTransaction(TransactionDefinition definition) {
        assertNotClosed();
        return Mono.from(this.connection.beginTransaction(definition)).doOnSubscribe(ignore -> this.inTransaction = true).doOnError(e -> this.inTransaction = false);
    }

    @Override
    public Mono<Void> close() {
        assertNotClosed();
        return this.release;
    }

    @Override
    public Mono<Void> commitTransaction() {
        assertNotClosed();
        return Mono.from(this.connection.commitTransaction()).doOnSubscribe(ignore -> this.inTransaction = false);
    }

    @Override
    public Batch createBatch() {
        assertNotClosed();
        return this.connection.createBatch();
    }

    @Override
    public Publisher<Void> createSavepoint(String s) {
        assertNotClosed();
        return this.connection.createSavepoint(s);
    }

    @Override
    public Statement createStatement(String s) {
        assertNotClosed();
        return this.connection.createStatement(s);
    }

    @Override
    public boolean isAutoCommit() {
        assertNotClosed();
        return this.connection.isAutoCommit();
    }

    @Override
    public ConnectionMetadata getMetadata() {
        assertNotClosed();
        return this.connection.getMetadata();
    }

    @Override
    public IsolationLevel getTransactionIsolationLevel() {
        assertNotClosed();
        return this.connection.getTransactionIsolationLevel();
    }

    @Override
    public Publisher<Void> releaseSavepoint(String s) {
        assertNotClosed();
        return this.connection.releaseSavepoint(s);
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return Mono.from(this.connection.rollbackTransaction()).doOnSubscribe(ignore -> this.inTransaction = false);
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String s) {
        return Mono.from(this.connection.rollbackTransactionToSavepoint(s));
    }

    @Override
    public Mono<Void> setAutoCommit(boolean autoCommit) {
        assertNotClosed();
        return Mono.from(this.connection.setAutoCommit(autoCommit));
    }

    @Override
    public Mono<Void> setLockWaitTimeout(Duration timeout) {
        assertNotClosed();
        return Mono.from(this.connection.setLockWaitTimeout(timeout));
    }

    @Override
    public Mono<Void> setStatementTimeout(Duration timeout) {
        assertNotClosed();
        return Mono.from(this.connection.setStatementTimeout(timeout));
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        assertNotClosed();
        return Mono.from(this.connection.setTransactionIsolationLevel(isolationLevel));
    }

    @Override
    public Connection unwrap() {
        return this.connection;
    }

    @Override
    public Publisher<Boolean> validate(ValidationDepth validationDepth) {
        return this.connection.validate(validationDepth);
    }

    private void assertNotClosed() {
        if (this.closed) {
            throw new IllegalStateException("The connection is closed");
        }
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append("[");
        sb.append(this.connection.toString());
        sb.append("]");
        return sb.toString();
    }
}
