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

import io.r2dbc.spi.*;
import io.r2dbc.spi.test.MockConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.util.ReflectionUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.time.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ConnectionPool}.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 * @author Petromir Dzhunev
 * @author Gabriel Calin
 */
@SuppressWarnings("unchecked")
final class ConnectionPoolUnitTests {

    @AfterEach
    void tearDown() {
        // clean up connection-pool mbeans
        JmxTestUtils.unregisterPoolMbeans();
    }

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
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);
            assertThat(Thread.currentThread().getName()).startsWith("single-");

        }).verifyComplete();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateConnectionAndOffloadPreparation() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class, withSettings().extraInterfaces(Wrapped.class));
        Wrapped<?> wrapped = (Wrapped<?>) connectionMock;

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        when(wrapped.unwrap(Scheduler.class)).thenReturn(Schedulers.parallel());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);
            assertThat(Thread.currentThread().getName()).startsWith("parallel-");
        }).verifyComplete();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateConnectionAndOffloadPreparationToExecutor() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class, withSettings().extraInterfaces(Wrapped.class));
        Wrapped<?> wrapped = (Wrapped<?>) connectionMock;

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        when(wrapped.unwrap(Executor.class)).thenReturn(ForkJoinPool.commonPool());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);
            assertThat(Thread.currentThread().getName()).startsWith("ForkJoinPool");
        }).verifyComplete();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void wrappedReturnsItself() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class, withSettings().extraInterfaces(Wrapped.class));
        Wrapped<?> wrapped = (Wrapped<?>) connectionMock;

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        when((Object) wrapped.unwrap(Scheduler.class)).thenReturn(connectionMock);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        Thread mainThread = Thread.currentThread();

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);
            assertThat(mainThread.getName()).isEqualTo(mainThread.getName());
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
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).allocatorSubscribeOn(Schedulers.immediate()).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.warmup().as(StepVerifier::create).expectNext(10).verifyComplete();
        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);

        }).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(creations).hasValue(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConsiderMinIdle() {

        AtomicInteger creations = new AtomicInteger();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnNext(it -> creations.incrementAndGet()));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).minIdle(5).initialSize(0).maxSize(10).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.warmup().as(StepVerifier::create).expectNext(5).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(creations).hasValue(5);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldConsiderCustomizer() {

        AtomicInteger creations = new AtomicInteger();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnNext(it -> creations.incrementAndGet()));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).allocatorSubscribeOn(Schedulers.immediate()).customizer(connectionPoolBuilder -> connectionPoolBuilder.sizeBetween(2, 10)).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().as(StepVerifier::create).consumeNextWith(actual -> {

            assertThat(actual).isInstanceOf(PooledConnection.class);
            assertThat(((Wrapped) actual).unwrap()).isSameAs(connectionMock);

        }).verifyComplete();

        verify(connectionFactoryMock).create();
        assertThat(creations).hasValue(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReusePooledConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        AtomicLong createCounter = new AtomicLong();
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnSubscribe(ignore -> createCounter.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).allocatorSubscribeOn(Schedulers.immediate()).initialSize(0).build();
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
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).allocatorSubscribeOn(Schedulers.immediate()).initialSize(0).build();
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
                Mono.delay(Duration.ofSeconds(5)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .maxCreateConnectionTime(Duration.ofSeconds(1))
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        StepVerifier.withVirtualTime(pool::create)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(2))
                .expectError(TimeoutException.class)
                .verify();

        verify(connectionFactoryMock).create();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldTimeoutCreateConnectionUsingZeroTimeout() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.defer(() ->
                Mono.delay(Duration.ofSeconds(15)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .maxCreateConnectionTime(Duration.ZERO)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        StepVerifier.withVirtualTime(pool::create)
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(11))
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
                Mono.delay(Duration.ofSeconds(15)).thenReturn(connectionMock))
        );
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .acquireRetry(0)
                .maxAcquireTime(Duration.ofSeconds(1))
                .build();

        StepVerifier.withVirtualTime(() -> new ConnectionPool(configuration).create())
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(11))
                .expectError(R2dbcTimeoutException.class)
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
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(1)
                .maxAcquireTime(Duration.ofMillis(10))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.warmup()
                .as(StepVerifier::create)
                .expectNext(1)
                .verifyComplete();

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
    void shouldTimeoutValidation() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.defer(() ->
                Mono.delay(Duration.ofSeconds(1)).thenReturn(connectionMock))
        );
        when(connectionMock.validate(any())).thenReturn(Mono.defer(() ->
                Mono.delay(Duration.ofSeconds(10)).thenReturn(false))
        );
        when(connectionMock.close()).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .acquireRetry(0)
                .maxValidationTime(Duration.ofSeconds(5))
                .maxAcquireTime(Duration.ofSeconds(15))
                .build();

        StepVerifier.withVirtualTime(() -> new ConnectionPool(configuration).create())
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(7))
                .expectError(R2dbcTimeoutException.class)
                .verify();

        verify(connectionFactoryMock).create();
        verify(connectionMock).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReusePooledConnectionAfterTimeout() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        AtomicInteger counter = new AtomicInteger();

        // create connection in order of fast, slow, fast, slow, ...
        Mono<Connection> connectionPublisher = Mono.defer(() -> {
            int count = counter.incrementAndGet();  // 1, 2, 3,...
            if (count % 2 == 0) {
                return Mono.delay(Duration.ofMillis(500)).thenReturn(connectionMock);  // slow creation
            }
            return Mono.just(connectionMock);  // fast creation
        });

        when(connectionFactoryMock.create()).thenReturn((Publisher) connectionPublisher);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .allocatorSubscribeOn(Schedulers.immediate())
                .acquireRetry(0)
                .initialSize(0)
                .maxAcquireTime(Duration.ofMillis(70))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        AtomicReference<Connection> firstConnectionHolder = new AtomicReference<>();

        // fast connection retrieval, do not close the connection yet, so that next call will create a new connection
        pool.create()
                .as(StepVerifier::create)
                .consumeNextWith(firstConnectionHolder::set)
                .verifyComplete();

        // slow connection retrieval
        pool.create()
                .as(StepVerifier::create)
                .expectError(R2dbcTimeoutException.class)
                .verify();

        assertThat(counter).hasValue(2);

        // now close the first connection. This put back the connection to the pool.
        StepVerifier.create(firstConnectionHolder.get().close()).verifyComplete();

        // This should retrieve from pool, not fetching from the connection publisher.
        pool.create()
                .as(StepVerifier::create)
                .assertNext(actual -> {
                    StepVerifier.create(actual.close()).verifyComplete();
                }).verifyComplete();

        assertThat(counter).hasValue(2);
    }

    @Test
    void shouldConsiderMaxIdleTime() {
        DelayClock delayClock = new DelayClock();
        SimplePoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        MockConnection secondConnection = MockConnection.builder().valid(true).build();

        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection, secondConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .clock(delayClock)
                .initialSize(0)
                .metricsRecorder(metricsRecorder)
                .maxIdleTime(Duration.ofDays(2))  // set idle to 2 days
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        delayClock.setDelay(Duration.ofDays(1));

        // should not be evicted
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);

        delayClock.setDelay(Duration.ofDays(3));

        // should be evicted and acquire new conn
        assertPoolCreatesConnectionSuccessfully(pool, secondConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(2);
    }

    @Test
    void shouldConsiderMaxIdleTimeWithDefault() {
        DelayClock delayClock = new DelayClock();
        SimplePoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        MockConnection secondConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection, secondConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .clock(delayClock)
                .initialSize(0)
                .metricsRecorder(metricsRecorder)
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        // should not be evicted
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        delayClock.setDelay(Duration.ofMinutes(30));

        // should be evicted and acquire new conn
        assertPoolCreatesConnectionSuccessfully(pool, secondConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(2);
    }

    @Test
    void shouldConsiderNegativeMaxIdleTime() {

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        MockConnection secondConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection, secondConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(0)
                .maxIdleTime(Duration.ofNanos(-1))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        // should not be evicted
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);
    }

    @Test
    void shouldConsiderBackgroundEvictionInterval() {

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(0)
                .maxIdleTime(Duration.ofMillis(200))
                .backgroundEvictionInterval(Duration.ofMillis(100))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);

        // should be evicted by background eviction.
        await("connection closing").atMost(1, TimeUnit.SECONDS).until(firstConnection::isCloseCalled);
    }

    @Test
    void shouldConsiderMaxIdleWithBackgroundEviction() {
        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(0)
                .maxIdleTime(Duration.ofMillis(200))
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);

        // should be evicted by background eviction.
        await("connection closing").atMost(1, TimeUnit.SECONDS).until(firstConnection::isCloseCalled);
    }

    @Test
    void shouldConsiderDisabledBackgroundEvictionInterval() throws InterruptedException {

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .initialSize(0)
                .maxIdleTime(Duration.ofMillis(200))
                .backgroundEvictionInterval(Duration.ZERO)
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        Thread.sleep(300);

        // should not be evicted
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);
        assertThat(firstConnection.isCloseCalled()).isFalse();
    }

    @Test
    void shouldConsiderMaxLifetime() {

        DelayClock delayClock = new DelayClock();
        SimplePoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        MockConnection secondConnection = MockConnection.builder().valid(true).build();

        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection, secondConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .clock(delayClock)
                .initialSize(0)
                .metricsRecorder(metricsRecorder)
                .maxLifeTime(Duration.ofDays(1))
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        // creating another connection should return the same connection
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        // set delay, so that first connection will expire
        delayClock.setDelay(Duration.ofDays(2));

        assertPoolCreatesConnectionSuccessfully(pool, secondConnection);

        // creating another connection should return the same connection
        assertPoolCreatesConnectionSuccessfully(pool, secondConnection);

        assertThat(connectionFactory.getCreateCount()).isEqualTo(2);
    }

    @Test
    void shouldConsiderMaxLifetimeWithDefault() {

        DelayClock delayClock = new DelayClock();
        SimplePoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
                .clock(delayClock)
                .initialSize(0)
                .metricsRecorder(metricsRecorder)
                .maxIdleTime(Duration.ofSeconds(-1))  // do not evict by idle time
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        delayClock.setDelay(Duration.ofDays(365));

        // after one year, it should not expire yet
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);
    }

    @Test
    void shouldReportMetrics() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        pool.warmup()
                .as(StepVerifier::create)
                .expectNext(10)
                .verifyComplete();

        assertThat(pool.getMetrics()).isPresent().hasValueSatisfying(actual -> {

            assertThat(actual.acquiredSize()).isZero();
            assertThat(actual.allocatedSize()).isNotZero().isEqualTo(configuration.getInitialSize());
            assertThat(actual.idleSize()).isNotZero().isEqualTo(configuration.getInitialSize());
        });

        Connection connection = pool.create().block(Duration.ZERO);

        assertThat(pool.getMetrics()).isPresent().hasValueSatisfying(actual -> {

            assertThat(actual.acquiredSize()).isEqualTo(1);
        });

        StepVerifier.create(connection.close()).verifyComplete();

        assertThat(pool.getMetrics()).isPresent().hasValueSatisfying(actual -> {

            assertThat(actual.acquiredSize()).isEqualTo(0);
        });
    }

    @Test
    void shouldRegisterToJmx() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .name("my-pool")
                .registerJmx(true)
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        assertThat(mBeanServer.getDomains()).contains(ConnectionPoolMXBean.DOMAIN);

        List<ObjectName> poolObjectNames = JmxTestUtils.getPoolMBeanNames();
        assertThat(poolObjectNames).hasSize(1);
        ObjectName objectName = poolObjectNames.get(0);
        assertThat(objectName.getKeyPropertyList())
                .hasSize(2)
                .containsEntry("name", "my-pool")
                .containsEntry("type", ConnectionPool.class.getSimpleName());
    }

    @Test
    void shouldNotRegisterToJmx() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .registerJmx(false)
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        assertThat(mBeanServer.getDomains()).doesNotContain(ConnectionPoolMXBean.DOMAIN);
    }

    @Test
    void shouldMBeanUnregisteredAtPoolDisposal() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        // pool.dispose() calls connection.close()
        when(connectionMock.close()).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .registerJmx(true)
                .name("my-pool")
                .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        assertThat(mBeanServer.getDomains()).contains(ConnectionPoolMXBean.DOMAIN);

        pool.dispose();

        assertThat(mBeanServer.getDomains()).doesNotContain(ConnectionPoolMXBean.DOMAIN);
    }

    @Test
    void shouldPropagateGracefullyDestroyHandlerFailure() {

        Connection connectionMock = mock(Connection.class);
        ConnectionPool pool = createConnectionPoolForDisposeTest(connectionMock);
        pool.warmup()
                .as(StepVerifier::create)
                .expectNext(10)
                .verifyComplete();

        IllegalArgumentException iae = new IllegalArgumentException();

        addDestroyHandler(pool, () -> {
            throw new IllegalStateException();
        });
        addDestroyHandler(pool, () -> {
            throw iae;
        });

        assertThatThrownBy(pool::dispose).isInstanceOf(IllegalStateException.class).hasSuppressedException(iae);
        verify(connectionMock, times(10)).close();
    }

    @Test
    void shouldPropagateGracefullyDestroyHandlerFailureOnDisposeLater() {

        Connection connectionMock = mock(Connection.class);
        ConnectionPool pool = createConnectionPoolForDisposeTest(connectionMock);

        pool.warmup()
                .as(StepVerifier::create)
                .expectNext(10)
                .verifyComplete();

        IllegalArgumentException iae = new IllegalArgumentException();

        addDestroyHandler(pool, () -> {
            throw new IllegalStateException();
        });
        addDestroyHandler(pool, () -> {
            throw iae;
        });

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        pool.disposeLater().as(StepVerifier::create).consumeErrorWith(thrown::set).verify();

        assertThat(thrown.get()).isInstanceOf(IllegalStateException.class).hasSuppressedException(iae);
        verify(connectionMock, times(10)).close();
    }

    @Test
    void disposedPoolShouldNoOpOnClose() {

        Connection connectionMock = mock(Connection.class);
        ConnectionPool pool = createConnectionPoolForDisposeTest(connectionMock);

        pool.warmup()
                .as(StepVerifier::create)
                .expectNext(10)
                .verifyComplete();

        pool.close().as(StepVerifier::create).verifyComplete();

        addDestroyHandler(pool, () -> {
            throw new IllegalStateException();
        });

        pool.disposeLater().as(StepVerifier::create).verifyComplete();
        verify(connectionMock, times(10)).close();
    }

    @Test
    void shouldDropConnectionOnFailedValidation() {

        AtomicInteger subscriptions = new AtomicInteger();
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock).doOnSubscribe(ignore -> subscriptions.incrementAndGet()));
        when(connectionMock.close()).thenReturn(Mono.empty());
        // first broken, retry broken, last success
        when(connectionMock.validate(ValidationDepth.LOCAL)).thenReturn(Mono.just(false), Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .allocatorSubscribeOn(Schedulers.immediate())
                .acquireRetry(0)
                .initialSize(0)
                .maxSize(2)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().flatMapMany(Connection::close).as(StepVerifier::create).verifyError();
        pool.create().flatMapMany(Connection::close).as(StepVerifier::create).verifyComplete();

        assertThat(subscriptions).hasValue(2);
    }

    @Test
    void shouldDropConnectionOnFailedValidationWithRetry() {

        AtomicInteger subscriptions = new AtomicInteger();
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock).doOnSubscribe(ignore -> subscriptions.incrementAndGet()));
        when(connectionMock.close()).thenReturn(Mono.empty());
        // first broken, retry broken, last success
        when(connectionMock.validate(ValidationDepth.LOCAL)).thenReturn(Mono.just(false), Mono.just(false), Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .allocatorSubscribeOn(Schedulers.immediate())
                .acquireRetry(1)
                .initialSize(0)
                .maxSize(2)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        pool.create().flatMapMany(Connection::close).as(StepVerifier::create).verifyError();
        pool.create().flatMapMany(Connection::close).as(StepVerifier::create).verifyComplete();

        assertThat(subscriptions).hasValue(3);
    }

    @Test
    void shouldRenderToString() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionFactoryMetadata metadataMock = mock(ConnectionFactoryMetadata.class);
        when(connectionFactoryMock.create()).thenReturn(Mono.empty());
        when(connectionFactoryMock.getMetadata()).thenReturn(metadataMock);
        when(metadataMock.getName()).thenReturn("H2");

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertThat(pool).hasToString("ConnectionPool[H2]");
    }

    @Test
    void shouldReportPoolInPool() {

        ConnectionPool connectionFactoryMock = mock(ConnectionPool.class);
        when(connectionFactoryMock.create()).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .initialSize(0)
                .build();

        new ConnectionPool(configuration);
    }

    @Test
    void shouldInvokeLifecyclePostAllocate() {

        AtomicBoolean wasCalled = new AtomicBoolean();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        when(connectionMock.postAllocate()).thenReturn(Mono.fromRunnable(() -> wasCalled.set(true)));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldInvokePostAllocate() {

        AtomicBoolean wasCalled = new AtomicBoolean();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .postAllocate(connection -> Mono.fromRunnable(() -> wasCalled.set(true)))
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(wasCalled).isTrue();
    }

    @Test
    void shouldInvokePostAllocateInOrder() {

        List<String> order = new ArrayList<>();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());
        when(connectionMock.postAllocate()).thenAnswer(it -> {
            order.add("Lifecycle.postAllocate");
            return Mono.fromRunnable(() -> order.add("Lifecycle.postAllocate.subscribe"));
        });

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .postAllocate(connection -> {
                    order.add("postAllocate");
                    return Mono.fromRunnable(() -> order.add("postAllocate.subscribe"));
                })
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        pool.create().as(StepVerifier::create).expectNextCount(1).verifyComplete();

        assertThat(order).containsExactly("Lifecycle.postAllocate", "Lifecycle.postAllocate.subscribe", "postAllocate", "postAllocate.subscribe");
    }

    /**
     * What is to be tested: a cancel while the pool prepares a connection, after {@code Lifecycle.postAllocate()} and
     * during the validation.
     * <p>
     * How will the test case be deemed successful and why: the validation is cancelled, the connection is closed once,
     * and no connection stays acquired. The connection's state is unknown in the middle of its preparation, so the pool
     * destroys it instead of returning it.
     * <p>
     * Why is it important to test this test case: this test pinned the opposite until #232 (it let the preparation
     * finish and returned the connection, since #140). The drain kept the protocol state consistent for a connection
     * that went back into the pool; a destroyed connection has no next borrower, and a validation that hangs no longer
     * keeps the connection in rotation.
     */
    @Test
    void cancelDuringAllocationShouldCancelPreparation() throws InterruptedException {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        CountDownLatch prepareLatch = new CountDownLatch(1);
        AtomicBoolean validationCancelled = new AtomicBoolean();
        AtomicInteger closes = new AtomicInteger();
        Mono<Void> prepare = Mono.<Void>empty().delayElement(Duration.ofMillis(100)).doOnSuccess(s -> prepareLatch.countDown());
        Mono<Boolean> validate = Mono.just(true).delayElement(Duration.ofMillis(250)).doOnCancel(() -> validationCancelled.set(true));

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(validate);
        when(connectionMock.postAllocate()).thenReturn(prepare);
        when(connectionMock.close()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> closes.incrementAndGet()));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        Disposable subscribe = pool.create().subscribe();
        assertThat(prepareLatch.await(2, TimeUnit.SECONDS)).isTrue();
        subscribe.dispose();

        PoolMetrics poolMetrics = pool.getMetrics().get();
        await().atMost(Duration.ofSeconds(2)).until(() -> closes.get() == 1);

        assertThat(validationCancelled).isTrue();
        assertThat(poolMetrics.pendingAcquireSize()).isEqualTo(0);
        assertThat(poolMetrics.acquiredSize()).isEqualTo(0);
        assertThat(closes).hasValue(1);
    }

    /**
     * What is to be tested: a cancel by {@code maxAcquireTime} while the validation of a new connection has not
     * answered, followed by a late answer of that validation.
     * <p>
     * How will the test case be deemed successful and why: the connection is closed, nothing stays allocated, and the
     * late answer changes nothing. The pool destroyed the connection at the cancel instead of returning it after the
     * validation, so nobody can pick it while the validation still runs.
     * <p>
     * Why is it important to test this test case: until now the connection went back into the pool at once while its
     * validation was still running, and MRU order picked the unresponsive connection again (#232).
     */
    @Test
    void cancelDuringAllocationShouldInvalidateConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        AtomicInteger closes = new AtomicInteger();

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));
        when(connectionMock.close()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> closes.incrementAndGet()));

        AtomicReference<Subscriber<? super Boolean>> subRef = new AtomicReference<>();
        Subscription subscription = new Subscription() {

            @Override
            public void request(long n) {

            }

            @Override
            public void cancel() {
            }
        };
        Publisher<Boolean> validationPublisher = subscriber -> {
            subscriber.onSubscribe(subscription);
            subRef.set(subscriber);
        };

        when(connectionMock.validate(any())).thenReturn(validationPublisher);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxAcquireTime(Duration.ofMillis(150)).acquireRetry(0)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        CompletableFuture<Connection> future = pool.create().toFuture();

        assertThatExceptionOfType(CompletionException.class).isThrownBy(future::join);

        PoolMetrics poolMetrics = pool.getMetrics().get();
        await().atMost(Duration.ofSeconds(2)).until(() -> closes.get() == 1 && poolMetrics.allocatedSize() == 0);

        subRef.get().onNext(true);
        subRef.get().onComplete();

        assertThat(poolMetrics.allocatedSize()).isEqualTo(0);
        assertThat(poolMetrics.idleSize()).isEqualTo(0);
        assertThat(closes).hasValue(1);
    }

    @Test
    void cancelDuringCreationShouldCloseConnection() {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        AtomicReference<Subscriber<? super Connection>> subRef = new AtomicReference<>();
        AtomicBoolean canceled = new AtomicBoolean();
        AtomicBoolean closed = new AtomicBoolean();
        Subscription subscription = new Subscription() {

            @Override
            public void request(long n) {

            }

            @Override
            public void cancel() {
                canceled.set(true);
            }
        };
        Publisher<Connection> connectionPublisher = subscriber -> {
            subscriber.onSubscribe(subscription);
            subRef.set(subscriber);
        };

        when(connectionMock.close()).thenReturn(Mono.<Void>empty().doOnSubscribe(it -> closed.set(true)));
        when(connectionFactoryMock.create()).thenReturn(Mono.from((Publisher) connectionPublisher));
        when(connectionMock.validate(any())).thenReturn(Mono.just(true));

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxCreateConnectionTime(Duration.ofMillis(1)).acquireRetry(0)
                .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        CompletableFuture<Connection> future = pool.create().toFuture();
        assertThatExceptionOfType(CompletionException.class).isThrownBy(future::join);

        subRef.get().onNext(connectionMock);
        subRef.get().onComplete();

        assertThat(closed).isTrue();
    }

    /**
     * What is to be tested: a subscriber that consumes the connection with {@code flatMap}, never closes it and is
     * cancelled after the hand-over, as Spring's {@code R2dbcTransactionManager.doBegin} is.
     * <p>
     * How will the test case be deemed successful and why: the connection is destroyed, its slot is free, and neither
     * the custom {@code preRelease} function nor {@code Lifecycle.preRelease()} ran. A call of the subscriber may still
     * run in the driver, so the pool neither cleans the connection up nor puts it back.
     * <p>
     * Why is it important to test this test case: until now the cancel returned the connection without cleanup, so a
     * transaction the subscriber had begun went to the next borrower (#211, spring-framework #36344). A cleanup
     * subscribed while the begin is in flight does not help on every driver.
     */
    @Test
    void cancelAfterHandOverShouldInvalidate() {

        AtomicInteger preReleases = new AtomicInteger();
        AtomicInteger lifecyclePreReleases = new AtomicInteger();
        ConnectionPool pool = createPoolForCancelTest(preReleases, lifecyclePreReleases);
        PoolMetrics poolMetrics = pool.getMetrics().get();

        Disposable subscription = pool.create().flatMap(connection -> Mono.never()).subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 1);

        subscription.dispose();

        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.allocatedSize() == 0);
        assertThat(preReleases).hasValue(0);
        assertThat(lifecyclePreReleases).hasValue(0);
    }

    /**
     * What is to be tested: a subscriber that closes the connection after the pool has taken it back on a cancel.
     * <p>
     * How will the test case be deemed successful and why: the late {@code close()} completes, the
     * {@code preRelease} function never ran, and the destroyed connection is not released. The cancel destroyed the
     * connection; the close after it has no effect.
     * <p>
     * Why is it important to test this test case: a subscriber may close in its own cleanup after the cancel. That
     * must neither fail nor release the connection a second time while another borrower may already have it.
     */
    @Test
    void closeAfterCancelShouldBeNoOp() {

        AtomicInteger preReleases = new AtomicInteger();
        ConnectionPool pool = createPoolForCancelTest(preReleases, new AtomicInteger());
        PoolMetrics poolMetrics = pool.getMetrics().get();
        AtomicReference<Connection> abandoned = new AtomicReference<>();

        Disposable subscription = pool.create().flatMap(connection -> {
            abandoned.set(connection);
            return Mono.never();
        }).subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 1);

        subscription.dispose();
        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.allocatedSize() == 0);

        Mono.from(abandoned.get().close()).as(StepVerifier::create).verifyComplete();
        assertThat(preReleases).hasValue(0);
        assertThat(poolMetrics.allocatedSize()).isZero();
    }

    /**
     * What is to be tested: a subscriber that closes the connection itself and is cancelled afterwards.
     * <p>
     * How will the test case be deemed successful and why: the {@code preRelease} function ran once, and the
     * connection is idle. The cancel after the close finds the connection released and does nothing.
     * <p>
     * Why is it important to test this test case: a cancel after the subscriber's own close must not release the
     * connection again; until now it called {@code ref.release()} a second time.
     */
    @Test
    void cancelAfterCloseShouldBeNoOp() {

        AtomicInteger preReleases = new AtomicInteger();
        ConnectionPool pool = createPoolForCancelTest(preReleases, new AtomicInteger());
        PoolMetrics poolMetrics = pool.getMetrics().get();

        Disposable subscription = pool.create().flatMap(connection -> Mono.from(connection.close()).then(Mono.never())).subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> preReleases.get() == 1 && poolMetrics.idleSize() == 1);

        subscription.dispose();

        assertThat(preReleases).hasValue(1);
        assertThat(poolMetrics.idleSize()).isEqualTo(1);
    }

    /**
     * What is to be tested: a subscriber that takes the first element and cancels from inside {@code onNext}, as
     * {@code Flux.next()}, {@code take(1)} and {@code awaitFirst()} do, and keeps the connection.
     * <p>
     * How will the test case be deemed successful and why: the connection stays acquired and usable, a second
     * {@code create()} on a pool of one does not get it, and it is idle once its subscriber closed it. The pool has
     * then left the connection with the subscriber that owns it.
     * <p>
     * Why is it important to test this test case: until now the cancel of such a subscriber returned the connection
     * while it was in use, and the next {@code create()} handed out the same database connection (#211).
     */
    @Test
    void firstElementSubscriberKeepsTheConnection() {

        ConnectionPool pool = createPoolForCancelTest(new AtomicInteger(), new AtomicInteger());
        PoolMetrics poolMetrics = pool.getMetrics().get();

        Connection connection = Flux.from(pool.create()).next().block(Duration.ofSeconds(2));

        assertThat(poolMetrics.acquiredSize()).isEqualTo(1);
        assertThatCode(() -> connection.createStatement("SELECT 1")).doesNotThrowAnyException();
        pool.create().as(StepVerifier::create).expectSubscription().expectNoEvent(Duration.ofMillis(100)).thenCancel().verify();

        Mono.from(connection.close()).as(StepVerifier::create).verifyComplete();
        assertThat(poolMetrics.idleSize()).isEqualTo(1);
    }

    /**
     * What is to be tested: many concurrent subscribers that consume the pool with {@code Mono.usingWhen} over a
     * {@code Flux}, which cancels the resource from inside its delivery. The scenario follows the test of #210.
     * <p>
     * How will the test case be deemed successful and why: no database connection is in use by two subscribers at
     * once. Every subscriber kept its connection until its own close.
     * <p>
     * Why is it important to test this test case: it is the defect of #211 as users meet it, with RxJava types, a
     * {@code Mono} made from a {@code Flux}, or {@code take}: two borrowers sharing one database connection.
     */
    @Test
    void shouldNotHandOutAConnectionTwice() {

        Set<Connection> inUse = ConcurrentHashMap.newKeySet();
        AtomicInteger duplicates = new AtomicInteger();
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.fromCallable(() -> {
            ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);
            when(connectionMock.validate(any())).thenReturn(Mono.just(true));
            when(connectionMock.postAllocate()).thenReturn(Mono.empty());
            when(connectionMock.preRelease()).thenReturn(Mono.empty());
            return connectionMock;
        }));

        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).build());

        Flux.range(0, 256)
            .flatMap(i -> Mono.usingWhen(Flux.defer(pool::create), connection -> {
                if (!inUse.add(((PooledConnection) connection).unwrap())) {
                    duplicates.incrementAndGet();
                }
                return Mono.delay(Duration.ofMillis(1));
            }, connection -> Mono.defer(() -> {
                inUse.remove(((PooledConnection) connection).unwrap());
                return Mono.from(connection.close());
            })), 32)
            .then()
            .as(StepVerifier::create)
            .expectComplete()
            .verify(Duration.ofSeconds(10));

        assertThat(duplicates).hasValue(0);
    }

    /**
     * What is to be tested: a {@code flatMap} whose mapper throws synchronously when it receives the connection.
     * <p>
     * How will the test case be deemed successful and why: the slot stays acquired. The mapper's failure cancels from
     * inside the delivery, which the pool cannot tell from a subscriber that keeps the connection, and nobody closes
     * it. This pins a known limit; it is not the behaviour one wants.
     * <p>
     * Why is it important to test this test case: the limit is a deliberate trade-off, see the pull request for #211.
     * The test makes any change to it visible, and it is to be changed together with that decision. Consuming
     * {@code create()} with {@code usingWhen}, as the README shows, avoids the case.
     */
    @Test
    void synchronousFailureInMapperKeepsTheSlot() {

        ConnectionPool pool = createPoolForCancelTest(new AtomicInteger(), new AtomicInteger());
        PoolMetrics poolMetrics = pool.getMetrics().get();

        pool.create().<Void>flatMap(connection -> {
            throw new IllegalStateException("mapper failed");
        }).as(StepVerifier::create).verifyError(IllegalStateException.class);

        assertThat(poolMetrics.acquiredSize()).isEqualTo(1);
    }

    /**
     * What is to be tested: a subscriber that is cancelled while {@code Lifecycle.postAllocate()} runs.
     * <p>
     * How will the test case be deemed successful and why: {@code postAllocate} is cancelled, the connection is closed,
     * nothing stays acquired or allocated, and neither the custom {@code preRelease} function nor
     * {@code Lifecycle.preRelease()} runs. The connection is destroyed, not released, so there is nothing to prepare
     * for a next borrower.
     * <p>
     * Why is it important to test this test case: until now the connection went back into the pool at once while its
     * preparation still ran on it, and another subscriber could acquire it in the meantime.
     */
    @Test
    void cancelDuringPostAllocateShouldInvalidate() {

        AtomicInteger preReleases = new AtomicInteger();
        AtomicInteger lifecyclePreReleases = new AtomicInteger();
        AtomicBoolean postAllocateCancelled = new AtomicBoolean();
        AtomicInteger closes = new AtomicInteger();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.just(true));
        when(connectionMock.postAllocate()).thenReturn(Mono.<Void>never().doOnCancel(() -> postAllocateCancelled.set(true)));
        when(connectionMock.preRelease()).thenReturn(Mono.fromRunnable(lifecyclePreReleases::incrementAndGet));
        when(connectionMock.close()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> closes.incrementAndGet()));

        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxSize(1)
                .preRelease(connection -> Mono.fromRunnable(preReleases::incrementAndGet))
                .build());
        PoolMetrics poolMetrics = pool.getMetrics().get();

        Disposable subscription = pool.create().subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 1);

        subscription.dispose();

        await().atMost(Duration.ofSeconds(2)).until(() -> closes.get() == 1);
        assertThat(postAllocateCancelled).isTrue();
        assertThat(poolMetrics.acquiredSize()).isEqualTo(0);
        assertThat(poolMetrics.allocatedSize()).isEqualTo(0);
        assertThat(preReleases).hasValue(0);
        assertThat(lifecyclePreReleases).hasValue(0);
    }

    /**
     * What is to be tested: a cancel that arrives while {@code postAllocate} fails, from two threads at once, 100 times.
     * <p>
     * How will the test case be deemed successful and why: in every round the connection is closed exactly once. The
     * cancel and the failed preparation both invalidate the connection, and the pool destroys a connection once.
     * <p>
     * Why is it important to test this test case: two paths now invalidate a connection that is being prepared. Two
     * destroys would close the connection twice and could count the permit back twice.
     */
    @Test
    void cancelRacingPreparationFailureShouldDestroyOnce() {

        for (int i = 0; i < 100; i++) {

            AtomicInteger closes = new AtomicInteger();
            TestPublisher<Void> postAllocate = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);

            ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
            ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

            when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
            when(connectionMock.validate(any())).thenReturn(Mono.just(true));
            when(connectionMock.postAllocate()).thenReturn(postAllocate);
            when(connectionMock.close()).thenReturn(Mono.<Void>empty().doOnSubscribe(ignore -> closes.incrementAndGet()));

            ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxSize(1).acquireRetry(0).build());
            PoolMetrics poolMetrics = pool.getMetrics().get();

            Disposable subscription = pool.create().subscribe(ignore -> {
            }, ignore -> {
            });
            await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 1);

            RaceTestUtils.race(() -> postAllocate.error(new IllegalStateException("postAllocate failed")), subscription::dispose);

            await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.allocatedSize() == 0);
            assertThat(closes).as("closes in round %d", i).hasValue(1);

            pool.dispose();
        }
    }

    /**
     * What is to be tested: a subscriber that gives up while the validation of its connection never answers, with no
     * {@code maxAcquireTime} and a driver whose {@code close()} never completes either.
     * <p>
     * How will the test case be deemed successful and why: the slot is free after the cancel, and on a pool of one a
     * second {@code create()} gets a new connection. The pool destroyed the hanging connection at the cancel, and the
     * permit comes back when the destroy starts, not when {@code close()} completes.
     * <p>
     * Why is it important to test this test case: without a timeout configured, nothing else bounds the preparation.
     * Had the pool waited for it, the slot would have stayed acquired for good.
     */
    @Test
    void hangingValidationShouldFreeTheSlotOnCancel() {

        Connection stalled = mock(Connection.class);
        when(stalled.validate(any())).thenReturn(Mono.never());
        when(stalled.close()).thenReturn(Mono.never());

        Connection healthy = mock(Connection.class);
        when(healthy.validate(any())).thenReturn(Mono.just(true));

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        // The pool subscribes to the publisher of create() once per new connection.
        AtomicInteger created = new AtomicInteger();
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.fromSupplier(() -> created.getAndIncrement() == 0 ? stalled : healthy));

        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxSize(1).build());
        PoolMetrics poolMetrics = pool.getMetrics().get();

        Disposable subscription = pool.create().subscribe();
        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 1);

        subscription.dispose();

        await().atMost(Duration.ofSeconds(2)).until(() -> poolMetrics.acquiredSize() == 0);
        verify(stalled).close();
        Mono.usingWhen(pool.create(), connection -> Mono.just(((PooledConnection) connection).unwrap()), Connection::close)
            .as(StepVerifier::create)
            .expectNext(healthy)
            .expectComplete()
            .verify(Duration.ofSeconds(2));
    }

    /**
     * What is to be tested: the scenario of #232: {@code maxAcquireTime} elapses while the validation of the
     * connection does not answer.
     * <p>
     * How will the test case be deemed successful and why: the first acquisition fails with the timeout, the
     * connection is closed, and the next acquisition gets another connection. The unresponsive connection is not
     * picked again.
     * <p>
     * Why is it important to test this test case: until now the timeout returned the unresponsive connection into the
     * pool, MRU order picked it for the next acquisition, and every acquisition timed out on the same connection while
     * a healthy one was never tried.
     */
    @Test
    void hangingValidationShouldNotBePickedAgain() {

        Connection stalled = mock(Connection.class);
        when(stalled.validate(any())).thenReturn(Mono.never());
        when(stalled.close()).thenReturn(Mono.empty());

        Connection healthy = mock(Connection.class);
        when(healthy.validate(any())).thenReturn(Mono.just(true));

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        // The pool subscribes to the publisher of create() once per new connection.
        AtomicInteger created = new AtomicInteger();
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.fromSupplier(() -> created.getAndIncrement() == 0 ? stalled : healthy));

        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxSize(1)
                .maxAcquireTime(Duration.ofMillis(100)).acquireRetry(0)
                .build());

        pool.create().as(StepVerifier::create).expectError(R2dbcTimeoutException.class).verify(Duration.ofSeconds(2));

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> verify(stalled).close());
        Mono.usingWhen(pool.create(), connection -> Mono.just(((PooledConnection) connection).unwrap()), Connection::close)
            .as(StepVerifier::create)
            .expectNext(healthy)
            .expectComplete()
            .verify(Duration.ofSeconds(2));
    }

    private ConnectionPool createPoolForCancelTest(AtomicInteger preReleases, AtomicInteger lifecyclePreReleases) {

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionWithLifecycle connectionMock = mock(ConnectionWithLifecycle.class);

        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.just(true));
        when(connectionMock.postAllocate()).thenReturn(Mono.empty());
        when(connectionMock.preRelease()).thenReturn(Mono.fromRunnable(lifecyclePreReleases::incrementAndGet));
        when(connectionMock.close()).thenReturn(Mono.empty());

        return new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(0).maxSize(1)
                .preRelease(connection -> Mono.fromRunnable(preReleases::incrementAndGet))
                .build());
    }

    interface ConnectionWithLifecycle extends Connection, Lifecycle {

    }

    private ConnectionPool createConnectionPoolForDisposeTest(Connection connectionMock) {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);

        // acquire time should also consider the time to obtain an actual connection
        when(connectionFactoryMock.create()).thenAnswer(it -> Mono.just(connectionMock));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        // pool.dispose() calls connection.close()
        when(connectionMock.close()).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();
        return new ConnectionPool(configuration);
    }

    private void addDestroyHandler(ConnectionPool pool, Runnable runnable) {
        Field field = ReflectionUtils.findField(ConnectionPool.class, "destroyHandlers");
        field.setAccessible(true);
        List<Runnable> destroyHandlers = (List<Runnable>) ReflectionUtils.getField(field, pool);
        destroyHandlers.add(runnable);
    }

    private void assertPoolCreatesConnectionSuccessfully(ConnectionPool pool, Connection expectedConnection) {
        pool.create().as(StepVerifier::create).assertNext(actual -> {
            assertThat(((Wrapped) actual).unwrap()).isSameAs(expectedConnection);
            StepVerifier.create(actual.close()).verifyComplete(); // make the connection to be released.
        }).verifyComplete();
    }

    /**
     * {@link Clock} that adds specified delay.
     *
     * @author Tadaya Tsuyukubo
     */
    private static class DelayClock extends Clock {

        private Duration delay = Duration.ZERO;

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant() {
            return Instant.now().plus(this.delay);
        }

        public void setDelay(Duration delay) {
            this.delay = delay;
        }

    }

    /**
     * {@link ConnectionFactory} that returns provided {@link Connection}s one by one.
     * Also, keeps how many times {@link #create()} is called.
     *
     * @author Tadaya Tsuyukubo
     */
    private static class CountingConnectionFactory implements ConnectionFactory {

        private final AtomicInteger createCounter = new AtomicInteger();

        private final List<Connection> connections = new ArrayList<>();

        public CountingConnectionFactory(Connection... connections) {
            this.connections.addAll(Arrays.asList(connections));
        }

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.defer(() -> {
                int count = this.createCounter.getAndIncrement();
                if (this.connections.size() <= count) {
                    return Mono.error(new RuntimeException(
                            format("ConnectionFactory#create is called %d times which is more than given connection size %d.",
                                    count + 1, this.connections.size())));
                }
                return Mono.just(this.connections.get(count));
            });
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            throw new UnsupportedOperationException();
        }

        /**
         * Number of times {@link #create()} is called.
         *
         * @return num of calls for {@link #create()} method.
         */
        public int getCreateCount() {
            return this.createCounter.get();
        }

    }

}
