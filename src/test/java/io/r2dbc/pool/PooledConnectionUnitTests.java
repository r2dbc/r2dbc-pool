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
import static org.mockito.ArgumentMatchers.anyBoolean;
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

    /**
     * Stub {@link PooledRef#release()} so that the returned flag tells whether the release into the pool was
     * subscribed. The method itself is called when the release path is assembled, also when it does not run.
     */
    private AtomicBoolean trackRelease() {
        AtomicBoolean released = new AtomicBoolean();
        when(pooledRefMock.release()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> released.set(true)));
        return released;
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

        when(connectionMock.isAutoCommit()).thenReturn(false);
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.fromRunnable(() -> order.add("rollbackTransaction.subscribe")));
        when(connectionMock.setAutoCommit(true)).thenReturn(Mono.fromRunnable(() -> order.add("setAutoCommit.subscribe")));

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> {
            order.add("preRelease");
            return Mono.fromRunnable(() -> order.add("preRelease.subscribe"));
        }, true);

        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(order).containsExactly("preRelease", "preRelease.subscribe", "rollbackTransaction.subscribe", "setAutoCommit.subscribe", "Lifecycle.preRelease",
            "Lifecycle.preRelease.subscribe");
    }

    /**
     * What is to be tested: the release of a connection that was created in auto-commit mode and is in manual-commit
     * mode when it is closed, without a transaction the pool saw begin.
     * <p>
     * How will the test case be deemed successful and why: the release calls {@code rollbackTransaction()} and then
     * {@code setAutoCommit(true)}, in that order, and returns the connection to the pool. Switching auto-commit on
     * commits an open transaction, so only a rollback first discards the work of the borrower.
     * <p>
     * Why is it important to test this test case: manual-commit mode may hide a transaction that a driver's begin, a
     * failed restore or code on the driver connection opened. Returning the connection in that mode lets the next
     * borrower's statements run inside it (#233).
     */
    @Test
    void shouldRestoreAutoCommitAfterRollback() {

        List<String> calls = new ArrayList<>();
        when(connectionMock.isAutoCommit()).thenReturn(false);
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.fromRunnable(() -> calls.add("rollbackTransaction")));
        when(connectionMock.setAutoCommit(true)).thenReturn(Mono.fromRunnable(() -> calls.add("setAutoCommit(true)")));

        PooledConnection connection = new PooledConnection(pooledRefMock, null, true);
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(calls).containsExactly("rollbackTransaction", "setAutoCommit(true)");
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: the release of a connection that is in the auto-commit mode it was created with and has no
     * transaction the pool saw begin.
     * <p>
     * How will the test case be deemed successful and why: neither {@code rollbackTransaction()} nor
     * {@code setAutoCommit(…)} is called, and the connection goes back into the pool. There is nothing to repair, so
     * the release must cost nothing.
     * <p>
     * Why is it important to test this test case: it is the common case. A round trip on every release would slow
     * down every borrower.
     */
    @Test
    void shouldNotTouchConnectionInBaselineState() {

        when(connectionMock.isAutoCommit()).thenReturn(true);

        PooledConnection connection = new PooledConnection(pooledRefMock, null, true);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, never()).rollbackTransaction();
        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: the release of a connection that was created in manual-commit mode and is in auto-commit
     * mode when it is closed.
     * <p>
     * How will the test case be deemed successful and why: the release calls {@code setAutoCommit(false)} and no
     * {@code rollbackTransaction()}. In auto-commit mode no transaction can be open, so there is nothing to roll back,
     * and the mode at creation is what the next borrower expects.
     * <p>
     * Why is it important to test this test case: a pool whose factory hands out connections in manual-commit mode
     * must keep them that way, also after a borrower switched auto-commit on.
     */
    @Test
    void shouldRestoreManualCommitBaseline() {

        when(connectionMock.isAutoCommit()).thenReturn(true);
        when(connectionMock.setAutoCommit(false)).thenReturn(Mono.empty());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, false);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).setAutoCommit(false);
        verify(connectionMock, never()).rollbackTransaction();
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: the release of a connection that was created in manual-commit mode and is still in it when
     * it is closed, without a transaction the pool saw begin.
     * <p>
     * How will the test case be deemed successful and why: the release calls {@code rollbackTransaction()} and no
     * {@code setAutoCommit(…)}, and the connection goes back into the pool. In manual-commit mode every statement of
     * the borrower may have opened a transaction implicitly, and the pool cannot see it.
     * <p>
     * Why is it important to test this test case: a pool whose factory hands out connections in manual-commit mode
     * would otherwise pass the uncommitted work and the locks of one borrower to the next one, who may commit them.
     */
    @Test
    void shouldRollbackManualCommitBaselineOnRelease() {

        when(connectionMock.isAutoCommit()).thenReturn(false);
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.empty());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, false);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: a release that rolls back a transaction the pool saw begin, while a call still running in
     * the driver switches auto-commit off only after the release has started.
     * <p>
     * How will the test case be deemed successful and why: the release reads the mode again after the rollback, sees
     * manual-commit mode, and calls {@code setAutoCommit(true)} after {@code rollbackTransaction()}. Had it decided on
     * the restore from the mode it read before the rollback, it would return the connection in manual-commit mode.
     * <p>
     * Why is it important to test this test case: a begin that is cancelled while it runs may switch auto-commit off
     * at any time; oracle-r2dbc does so in the first step of the begin.
     */
    @Test
    void shouldReadAutoCommitAgainAfterRollback() {

        List<String> calls = new ArrayList<>();
        AtomicBoolean autoCommit = new AtomicBoolean(true);
        when(connectionMock.isAutoCommit()).thenAnswer(it -> autoCommit.get());
        // The begin still in flight switches auto-commit off before the driver runs the rollback queued behind it.
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.fromRunnable(() -> {
            autoCommit.set(false);
            calls.add("rollbackTransaction");
        }));
        when(connectionMock.setAutoCommit(true)).thenReturn(Mono.fromRunnable(() -> calls.add("setAutoCommit(true)")));

        PooledConnection connection = new PooledConnection(pooledRefMock, null, true);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(calls).containsExactly("rollbackTransaction", "setAutoCommit(true)");
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: a release whose rollback never completes, with {@code maxReleaseTime} set.
     * <p>
     * How will the test case be deemed successful and why: after {@code maxReleaseTime} the connection is invalidated,
     * it is not released into the pool, and {@code close()} completes. A connection whose rollback did not come back
     * is in a state nobody knows; destroying it frees the slot.
     * <p>
     * Why is it important to test this test case: without a bound the slot stays acquired for good, and no eviction
     * applies to an acquired connection (#234). The rollback runs exactly on the connections that are already in
     * trouble.
     */
    @Test
    void hangingRollbackShouldInvalidateAfterMaxReleaseTime() {

        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicBoolean released = trackRelease();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidated.set(true)));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.never());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, null, Duration.ofSeconds(2));
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        StepVerifier.withVirtualTime(connection::close).expectSubscription().thenAwait(Duration.ofSeconds(2)).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(invalidated).isTrue();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: a release whose user {@code preRelease} function never completes, with
     * {@code maxReleaseTime} set.
     * <p>
     * How will the test case be deemed successful and why: after {@code maxReleaseTime} the connection is invalidated
     * and not released, and {@code close()} completes. The bound covers every step before the release, not only the
     * pool's own.
     * <p>
     * Why is it important to test this test case: an application function that hangs costs a slot as surely as a
     * rollback that hangs. Applications bound their own function today only because the pool does not.
     */
    @Test
    void hangingPreReleaseShouldInvalidateAfterMaxReleaseTime() {

        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicBoolean released = trackRelease();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidated.set(true)));

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> Mono.never(), null, Duration.ofSeconds(2));

        StepVerifier.withVirtualTime(connection::close).expectSubscription().thenAwait(Duration.ofSeconds(2)).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(invalidated).isTrue();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: a release that times out on a connection whose destroy never completes either.
     * <p>
     * How will the test case be deemed successful and why: {@code close()} completes after {@code maxReleaseTime},
     * although the invalidation it started never completes. The pool starts the destroy and does not wait for it; the
     * permit comes back when the destroy starts.
     * <p>
     * Why is it important to test this test case: a driver whose call hangs often does not complete {@code close()}
     * either. A bounded release that then waits for an unbounded destroy bounds nothing for the caller, and with
     * Spring's transaction manager the caller is the request (#234).
     */
    @Test
    void releaseTimeoutShouldNotWaitForDestroy() {

        AtomicBoolean invalidationStarted = new AtomicBoolean();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>never().doOnSubscribe(ignore -> invalidationStarted.set(true)));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.never());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, null, Duration.ofSeconds(2));
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        StepVerifier.withVirtualTime(connection::close).expectSubscription().thenAwait(Duration.ofSeconds(2)).expectComplete().verify(Duration.ofSeconds(5));

        assertThat(invalidationStarted).isTrue();
    }

    /**
     * What is to be tested: a release whose rollback never completes, without {@code maxReleaseTime}.
     * <p>
     * How will the test case be deemed successful and why: {@code close()} has not completed after a day of virtual
     * time, and the connection was neither invalidated nor released. Without the option, nothing bounds the release,
     * as before.
     * <p>
     * Why is it important to test this test case: the option is off by default, so that existing configurations
     * behave as they do today. A timeout that applies without being asked for would destroy connections that are only
     * slow.
     */
    @Test
    void releaseWithoutMaxReleaseTimeIsUnbounded() {

        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicBoolean released = trackRelease();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidated.set(true)));
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.never());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, null, ConnectionPoolConfiguration.NO_TIMEOUT);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();

        StepVerifier.withVirtualTime(connection::close).expectSubscription().expectNoEvent(Duration.ofDays(1)).thenCancel().verify();

        assertThat(invalidated).isFalse();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: a release whose rollback fails.
     * <p>
     * How will the test case be deemed successful and why: {@code setAutoCommit(…)} is never called, the connection is
     * invalidated, and it is not returned to the pool. After a failed rollback the state of the session is unknown,
     * and switching auto-commit on could commit what the rollback was meant to discard.
     * <p>
     * Why is it important to test this test case: until now a failed rollback was swallowed and the connection went
     * back into the pool, which is how a pool gets poisoned.
     */
    @Test
    void failedRollbackShouldInvalidateWithoutRestore() {

        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicBoolean released = trackRelease();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidated.set(true)));
        when(connectionMock.isAutoCommit()).thenReturn(false);
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.error(new RuntimeException("rollback failed")));

        PooledConnection connection = new PooledConnection(pooledRefMock, null, true);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        assertThat(invalidated).isTrue();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: a release whose restore of the auto-commit mode fails.
     * <p>
     * How will the test case be deemed successful and why: the connection is invalidated and not returned to the pool.
     * A connection in the wrong mode must not reach the next borrower.
     * <p>
     * Why is it important to test this test case: the restore is the step that guarantees the mode; if it may fail
     * silently, the guarantee is gone.
     */
    @Test
    void failedRestoreShouldInvalidate() {

        AtomicBoolean invalidated = new AtomicBoolean();
        AtomicBoolean released = trackRelease();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidated.set(true)));
        when(connectionMock.isAutoCommit()).thenReturn(false);
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.empty());
        when(connectionMock.setAutoCommit(true)).thenReturn(Mono.error(new RuntimeException("restore failed")));

        PooledConnection connection = new PooledConnection(pooledRefMock, null, true);
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(invalidated).isTrue();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: that the release reads the auto-commit mode after a user {@code preRelease} function ran.
     * <p>
     * How will the test case be deemed successful and why: the connection is in manual-commit mode until the
     * {@code preRelease} function switches it back, no transaction was begun, and the release calls neither
     * {@code rollbackTransaction()} nor {@code setAutoCommit(…)}. Had it read the mode before, it would see a
     * deviation and repair the connection a second time.
     * <p>
     * Why is it important to test this test case: applications repair connections in {@code preRelease} today. The
     * pool must work next to such a function without doing its work twice.
     */
    @Test
    void shouldReadAutoCommitAfterUserPreRelease() {

        AtomicBoolean autoCommit = new AtomicBoolean(false);
        when(connectionMock.isAutoCommit()).thenAnswer(it -> autoCommit.get());

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> Mono.fromRunnable(() -> autoCommit.set(true)), true);
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, never()).rollbackTransaction();
        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: the same as {@link #shouldReadAutoCommitAfterUserPreRelease()}, with a transaction that
     * was begun and not committed.
     * <p>
     * How will the test case be deemed successful and why: the release calls {@code rollbackTransaction()} once and no
     * {@code setAutoCommit(…)}. The {@code preRelease} function works on the driver connection and cannot clear the
     * pool's flag, so the pool rolls back once more; the mode is already right.
     * <p>
     * Why is it important to test this test case: an application function and the pool's reset run side by side
     * while applications migrate. The second rollback is harmless, a restore would not be.
     */
    @Test
    void shouldRollbackOnceMoreAfterUserPreReleaseWhenInTransaction() {

        AtomicBoolean autoCommit = new AtomicBoolean(false);
        when(connectionMock.isAutoCommit()).thenAnswer(it -> autoCommit.get());
        when(connectionMock.rollbackTransaction()).thenReturn(Mono.empty());

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> Mono.fromRunnable(() -> autoCommit.set(true)), true);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock, times(1)).rollbackTransaction();
        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        verify(pooledRefMock).release();
    }

    /**
     * What is to be tested: a release whose user {@code preRelease} function fails.
     * <p>
     * How will the test case be deemed successful and why: the connection is invalidated once, and neither the
     * rollback, nor {@code Lifecycle.preRelease()}, nor the release into the pool runs. The invalidation closes the
     * connection, so every further step would act on a closed connection and fail a second time for the same cause.
     * <p>
     * Why is it important to test this test case: until now the release went on after the invalidation, and a
     * connection whose cleanup failed could go back into the pool.
     */
    @Test
    void failedPreReleaseShouldStopTheRelease() {

        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);
        reset(pooledRefMock);
        setup(connectionMock);
        AtomicBoolean released = trackRelease();

        AtomicInteger invalidations = new AtomicInteger();
        when(pooledRefMock.invalidate()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> invalidations.incrementAndGet()));
        when(connectionMock.isAutoCommit()).thenReturn(false);

        PooledConnection connection = new PooledConnection(pooledRefMock, c -> Mono.error(new RuntimeException("preRelease failed")), true);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.close().as(StepVerifier::create).verifyComplete();

        assertThat(invalidations).hasValue(1);
        verify(connectionMock, never()).rollbackTransaction();
        verify(connectionMock, never()).preRelease();
        assertThat(released).as("released into the pool").isFalse();
    }

    /**
     * What is to be tested: the release of a connection whose auto-commit mode at creation is unknown.
     * <p>
     * How will the test case be deemed successful and why: the release rolls back the transaction that was begun,
     * never reads {@code isAutoCommit()} and never calls {@code setAutoCommit(…)}. Without a baseline there is nothing
     * to compare with, so the release behaves as before.
     * <p>
     * Why is it important to test this test case: the mode is unknown when reading it at creation failed, and for a
     * {@code PooledConnection} created without one. Those connections must not fail on a read the release does not
     * need.
     */
    @Test
    void unknownBaselineKeepsTodaysBehaviour() {

        when(connectionMock.rollbackTransaction()).thenReturn(Mono.empty());

        PooledConnection connection = new PooledConnection(pooledRefMock, null, null);
        connection.beginTransaction().as(StepVerifier::create).verifyComplete();
        connection.close().as(StepVerifier::create).verifyComplete();

        verify(connectionMock).rollbackTransaction();
        verify(connectionMock, never()).isAutoCommit();
        verify(connectionMock, never()).setAutoCommit(anyBoolean());
        verify(pooledRefMock).release();
    }

    interface ConnectionWithLifecycle extends Connection, Lifecycle {

    }

}
