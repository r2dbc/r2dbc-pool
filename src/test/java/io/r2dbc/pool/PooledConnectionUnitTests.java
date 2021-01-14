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
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.pool.PooledRef;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
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
}
