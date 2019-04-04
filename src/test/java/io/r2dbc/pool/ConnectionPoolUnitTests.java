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

import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionPool}.
 */
@SuppressWarnings("unchecked")
final class ConnectionPoolUnitTests {

    @Test
    void shouldReturnOriginalMetadata() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionFactoryMetadata metadata = mock(ConnectionFactoryMetadata.class);
        when(connectionFactoryMock.create()).thenReturn((Mono) Mono.just(ConnectionFactory.class));
        when(connectionFactoryMock.getMetadata()).thenReturn(metadata);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertThat(pool.getMetadata()).isSameAs(metadata);
    }

    @Test
    void shouldUnwrapOriginalFactory() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        when(connectionFactoryMock.create()).thenReturn((Mono) Mono.just(ConnectionFactory.class));

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
    void shouldConsiderInitialSize() {

        AtomicInteger creations = new AtomicInteger();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnNext(it -> creations.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);

        }).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(creations).hasValue(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReusePooledConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        AtomicLong createCounter = new AtomicLong();
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnSubscribe(ignore -> createCounter.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).build();
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

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();
        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(createCounter).hasValue(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldTimeoutCreateConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.defer(() ->
                Mono.delay(Duration.ofMillis(100)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(0)
                .maxCreateConnectionTime(Duration.ofMillis(10))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);


        pool.create().as(StepVerifier::create)
                .expectSubscription()
                .expectError(TimeoutException.class)
                .verify();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldTimeoutAcquireConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.defer(() ->
                Mono.delay(Duration.ofMillis(100)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(0)
                .maxAcquireTime(Duration.ofMillis(10))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create)
                .expectError(TimeoutException.class)
                .verify();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotTimeoutAcquireConnectionWhenPooled() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.defer(() ->
                Mono.delay(Duration.ofMillis(100)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(1)
                .maxAcquireTime(Duration.ofMillis(10))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        // When initial size of the pool is non-zero, even though creating connection is slow,
        // once connection is in pool, acquiring a connection from pool is fast.
        // Therefore, it should not timeout for acquiring a connection from pool.

        pool.create().as(StepVerifier::create)
                .expectNextCount(1)
                .verifyComplete();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReusePooledConnectionAfterTimeout() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        AtomicInteger counter = new AtomicInteger();

        // create connection in order of fast, slow, fast, slow, ...
        Mono<Connection> connectionPublisher = Mono.defer(() -> {
            int count = counter.incrementAndGet();  // 1, 2, 3,...
            if (count % 2 == 0) {
                return Mono.delay(Duration.ofMillis(100)).thenReturn(connectionMock);  // slow creation
            }
            return Mono.just(connectionMock);  // fast creation
        });

        when(connectionFactoryMock.create()).thenReturn((Publisher)connectionPublisher);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(0)
                .maxAcquireTime(Duration.ofMillis(70))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);


        AtomicReference<Connection> firstConnectionHolder = new AtomicReference<>();

        // fast connection retrieval, do not close the connection yet, so that next call will create a new connection
        pool.create().as(StepVerifier::create).consumeNextWith(firstConnectionHolder::set).verifyComplete();

        // slow connection retrieval
        pool.create().as(StepVerifier::create)
                .expectError(TimeoutException.class)
                .verify();

        assertThat(counter).hasValue(2);

        // now close the first connection. This put back the connection to the pool.
        StepVerifier.create(firstConnectionHolder.get().close()).verifyComplete();

        // This should retrieve from pool, not fetching from the connection publisher.
        pool.create().as(StepVerifier::create).assertNext(actual -> {
            StepVerifier.create(actual.close()).verifyComplete();
        }).verifyComplete();

        assertThat(counter).hasValue(2);
    }

}
