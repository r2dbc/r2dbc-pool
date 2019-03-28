/*
 * Copyright 2019 the original author or authors.
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
import io.r2dbc.spi.Wrapped;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionPool}.
 */
final class ConnectionPoolUnitTests {

    @Test
    void shouldReturnOriginalMetadata() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionFactoryMetadata metadata = mock(ConnectionFactoryMetadata.class);
        when(connectionFactoryMock.create()).thenReturn(Mono.empty());
        when(connectionFactoryMock.getMetadata()).thenReturn(metadata);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertThat(pool.getMetadata()).isSameAs(metadata);
    }

    @Test
    void shouldUnwrapOriginalFactory() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        when(connectionFactoryMock.create()).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertThat(pool.unwrap()).isSameAs(connectionFactoryMock);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);

        }).verifyComplete();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReusePooledConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        AtomicLong createCounter = new AtomicLong();
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnSubscribe(ignore -> createCounter.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).assertNext(actual -> {
            StepVerifier.create(actual.close()).verifyComplete();
        }).verifyComplete();

        pool.create().as(StepVerifier::create).assertNext(actual -> {
            StepVerifier.create(actual.close()).verifyComplete();
        }).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(createCounter).hasValue(1);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateMultipleConnections() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        AtomicLong createCounter = new AtomicLong();
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnSubscribe(ignore -> createCounter.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();
        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(createCounter).hasValue(2);
    }
}
