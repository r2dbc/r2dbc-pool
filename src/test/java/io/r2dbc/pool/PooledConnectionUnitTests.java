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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Lifecycle;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.pool.PooledRef;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PooledConnection}.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("unchecked")
class PooledConnectionUnitTests {

    Connection connectionMock = mock(Connection.class);

    PooledRef<Connection> pooledRefMock = mock(PooledRef.class);

    @BeforeEach
    void setUp() {
        setup(connectionMock);
    }

    private void setup(Connection connectionMock) {
        when(pooledRefMock.poolable()).thenReturn(connectionMock);
        when(pooledRefMock.release()).thenReturn(Mono.empty());
        when(connectionMock.beginTransaction()).thenReturn(Mono.empty());
        when(connectionMock.beginTransaction(any())).thenReturn(Mono.empty());
        when(connectionMock.close()).thenReturn(Mono.empty());
        when(connectionMock.validate(ValidationDepth.LOCAL)).thenReturn(Mono.empty());
    }

    @Test
    void shouldRollbackUnfinishedTransaction() {

        AtomicBoolean wasCalled = new AtomicBoolean();
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldRollbackUnfinishedExtendedTransaction() {

        AtomicBoolean wasCalled = new AtomicBoolean();
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction(mock(TransactionDefinition.class)).as(StepVerifier::create).verifyComplete();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldPristineTransactionLeavesTransactionalStateAsIs() {

        AtomicInteger rollbacks = new AtomicInteger();
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbacks.incrementAndGet()));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, never()).rollbackTransaction();
        assertThat(rollbacks).hasValue(0);
    }

    @Test
    void committedTransactionLeavesTransactionalStateAsIs() {

        when(connectionMock.commitTransaction()).thenReturn(Mono.empty());

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.commitTransaction().as(StepVerifier::create).verifyComplete();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, never()).rollbackTransaction();
    }

    /**
     * What is to be tested: that {@link PooledConnection#close()} rolls back after {@code commitTransaction()} failed.
     * <p>
     * How will the test case be deemed successful and why: {@code rollbackTransaction()} of the driver connection is
     * called and subscribed. A failed commit leaves the transaction open on the server, and only the rollback ends it
     * before the connection goes back into the pool.
     * <p>
     * Why is it important to test this test case: when the commit cleared the flag on subscription, a failed commit,
     * for example a serialization failure or a deadlock, returned the connection with its transaction open. The next
     * borrower's statements then ran inside it, and its commit or rollback also decided the failed borrower's work
     * (#228, #233).
     */
    @Test
    void failedCommitShouldRollbackOnClose() {

        AtomicBoolean rollbackCalled = new AtomicBoolean();
        when(connectionMock.commitTransaction()).thenReturn(Mono.error(new RuntimeException("commit failed")));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbackCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.commitTransaction().as(StepVerifier::create).verifyError();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbackCalled).isTrue();
    }

    /**
     * What is to be tested: that {@link PooledConnection#close()} rolls back after {@code beginTransaction()} failed.
     * <p>
     * How will the test case be deemed successful and why: {@code rollbackTransaction()} of the driver connection is
     * called and subscribed. Only then is whatever part of the begin the driver completed ended before the connection
     * goes back into the pool.
     * <p>
     * Why is it important to test this test case: a driver may complete part of a begin before it fails. oracle-r2dbc
     * switches auto-commit off and then sends {@code SET TRANSACTION}. Without the rollback the connection returns in
     * manual-commit mode, and on Oracle every further begin on it fails with {@code ORA-01453} (#233).
     */
    @Test
    void failedBeginShouldRollbackOnClose() {

        AtomicBoolean rollbackCalled = new AtomicBoolean();
        when(connectionMock.beginTransaction()).thenReturn(Mono.error(new RuntimeException("begin failed")));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbackCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyError();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbackCalled).isTrue();
    }

    /**
     * What is to be tested: the same as {@link #failedBeginShouldRollbackOnClose()} for
     * {@code beginTransaction(TransactionDefinition)}.
     * <p>
     * How will the test case be deemed successful and why: {@code rollbackTransaction()} of the driver connection is
     * called and subscribed, so the failed begin is ended before the connection goes back into the pool.
     * <p>
     * Why is it important to test this test case: this overload has its own implementation, and it is the one Spring's
     * {@code R2dbcTransactionManager} uses.
     */
    @Test
    void failedExtendedBeginShouldRollbackOnClose() {

        AtomicBoolean rollbackCalled = new AtomicBoolean();
        when(connectionMock.beginTransaction(any())).thenReturn(Mono.error(new RuntimeException("begin failed")));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbackCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction(mock(TransactionDefinition.class)).as(StepVerifier::create).verifyError();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbackCalled).isTrue();
    }

    /**
     * What is to be tested: that {@link PooledConnection#close()} rolls back after the subscriber of
     * {@code beginTransaction()} cancelled before the begin completed.
     * <p>
     * How will the test case be deemed successful and why: {@code rollbackTransaction()} of the driver connection is
     * called and subscribed. A cancel says nothing about how far the begin got, and a driver may finish it anyway, so
     * the flag has to stay set.
     * <p>
     * Why is it important to test this test case: a timeout that cancels a begin in flight is how a connection is left
     * with an open transaction in the first place (#233, #211). The behaviour holds today; the test keeps a later change,
     * for example a reset of the flag on cancel, from losing it.
     */
    @Test
    void cancelledBeginShouldRollbackOnClose() {

        AtomicBoolean rollbackCalled = new AtomicBoolean();
        when(connectionMock.beginTransaction()).thenReturn(Mono.never());
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbackCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).thenCancel().verify();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbackCalled).isTrue();
    }

    /**
     * What is to be tested: that {@link PooledConnection#close()} rolls back after the subscriber of
     * {@code commitTransaction()} cancelled before the commit completed.
     * <p>
     * How will the test case be deemed successful and why: {@code rollbackTransaction()} of the driver connection is
     * called and subscribed. The outcome of a cancelled commit is unknown: a rollback after a commit that went through
     * changes nothing, a skipped rollback after one that did not leaves the transaction open.
     * <p>
     * Why is it important to test this test case: a timeout around the commit is common, and before the commit cleared
     * the flag only on success it returned such a connection with its transaction open. It complements
     * {@code failedCommitShouldRollbackOnClose()} from #228.
     */
    @Test
    void cancelledCommitShouldRollbackOnClose() {

        AtomicBoolean rollbackCalled = new AtomicBoolean();
        when(connectionMock.commitTransaction()).thenReturn(Mono.never());
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbackCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.commitTransaction().as(StepVerifier::create).thenCancel().verify();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbackCalled).isTrue();
    }

    /**
     * What is to be tested: that {@link PooledConnection#close()} rolls back after a {@code rollbackTransaction()} of the
     * borrower failed.
     * <p>
     * How will the test case be deemed successful and why: the driver's {@code rollbackTransaction()} is called a
     * second time by the release. A failed rollback says nothing about whether the transaction ended, so the flag has to
     * stay set.
     * <p>
     * Why is it important to test this test case: the rollback cleared the flag on subscription, as the commit did
     * before #228, so a failed rollback returned the connection with its transaction open.
     */
    @Test
    void failedRollbackShouldRollbackOnClose() {

        AtomicInteger rollbacks = new AtomicInteger();
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.error(new RuntimeException("rollback failed")),
            Mono.<Void>empty().doOnSuccess(o -> rollbacks.incrementAndGet()));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.rollbackTransaction().as(StepVerifier::create).verifyError();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, times(2)).rollbackTransaction();
        assertThat(rollbacks).hasValue(1);
    }

    @Test
    void rolledBackTransactionLeavesTransactionalStateAsIs() {

        AtomicInteger rollbacks = new AtomicInteger();
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.<Void>empty().doOnSuccess(o -> rollbacks.incrementAndGet()));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.rollbackTransaction().as(StepVerifier::create).verifyComplete();

        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        assertThat(rollbacks).hasValue(1);
    }

    @Test
    void shouldInvalidateReferenceForBrokenConnection() {

        AtomicBoolean released = new AtomicBoolean();

        reset(connectionMock);
        when(connectionMock.validate(ValidationDepth.LOCAL)).thenReturn(Mono.error(new IllegalStateException()));
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> released.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);

        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(released).isTrue();
    }

    @Test
    void shouldCallSetAutoCommit() {

        AtomicBoolean wasCalled = new AtomicBoolean();
        when(connectionMock.setAutoCommit(true)).thenReturn(Mono.<Void>empty().doOnSuccess(o -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.setAutoCommit(true).as(StepVerifier::create).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldCallSetLockWaitTimeout() {

        AtomicBoolean wasCalled = new AtomicBoolean();
        when(connectionMock.setLockWaitTimeout(Duration.ofSeconds(10))).thenReturn(Mono.<Void>empty().doOnSuccess(o -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.setLockWaitTimeout(Duration.ofSeconds(10)).as(StepVerifier::create).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldCallSetStatementTimeout() {

        AtomicBoolean wasCalled = new AtomicBoolean();
        when(connectionMock.setStatementTimeout(Duration.ofSeconds(10))).thenReturn(Mono.<Void>empty().doOnSuccess(o -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.setStatementTimeout(Duration.ofSeconds(10)).as(StepVerifier::create).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldInvokeLifecyclePreRelease() {

        AtomicBoolean wasCalled = new AtomicBoolean();

        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);
        reset(pooledRefMock);
        setup(connectionMock);

        when(connectionMock.preRelease()).thenReturn(Mono.fromRunnable(() -> wasCalled.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock);
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldInvokePreRelease() {

        AtomicBoolean wasCalled = new AtomicBoolean();

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> Mono.fromRunnable(() -> wasCalled.set(true)));
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldInvokePreReleaseInOrder() {

        List<String> order = new ArrayList<>();

        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);
        reset(pooledRefMock);
        setup(connectionMock);

        when(connectionMock.preRelease()).thenAnswer(it -> {
            order.add("Lifecycle.preRelease");
            return Mono.fromRunnable(() -> order.add("Lifecycle.preRelease.subscribe"));
        });

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> {
            order.add("preRelease");
            return Mono.fromRunnable(() -> order.add("preRelease.subscribe"));
        });

        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(order).containsExactly("preRelease", "preRelease.subscribe", "Lifecycle.preRelease", "Lifecycle.preRelease.subscribe");
    }

    interface ConnectionWithLifecycle extends Connection, Lifecycle {

    }

}
