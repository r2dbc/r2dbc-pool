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
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.ValidationDepth;
import io.r2dbc.spi.Wrapped;
import io.r2dbc.spi.test.MockConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionPool}.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
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
    void shouldConsiderCustomizer() {

        AtomicInteger creations = new AtomicInteger();

        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        Connection connectionMock = mock(Connection.class);
        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock).doOnNext(it -> creations.incrementAndGet()));
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).customizer(connectionPoolBuilder -> connectionPoolBuilder.sizeBetween(2, 10)).build();
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
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

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
            Mono.delay(Duration.ofDays(1)).thenReturn(connectionMock))
        );

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .maxCreateConnectionTime(Duration.ofMinutes(10))
            .build();

        ConnectionPool pool = new ConnectionPool(configuration);

        StepVerifier.withVirtualTime(pool::create)
            .expectSubscription()
            .thenAwait(Duration.ofMinutes(11))
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
            Mono.delay(Duration.ofDays(1)).thenReturn(connectionMock))
        );
        when(connectionMock.validate(any())).thenReturn(Mono.empty());

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .acquireRetry(0)
            .maxAcquireTime(Duration.ofMinutes(10))
            .build();

        StepVerifier.withVirtualTime(() -> new ConnectionPool(configuration).create())
            .expectSubscription()
            .thenAwait(Duration.ofMinutes(11))
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
    void shouldConsiderMaxIdleTimeWithZero() {

        MockConnection firstConnection = MockConnection.builder().valid(true).build();
        MockConnection secondConnection = MockConnection.builder().valid(true).build();
        CountingConnectionFactory connectionFactory = new CountingConnectionFactory(firstConnection, secondConnection);

        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
            .initialSize(0)
            .maxIdleTime(Duration.ZERO)
            .build();
        ConnectionPool pool = new ConnectionPool(configuration);

        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);

        // should not be evicted
        assertPoolCreatesConnectionSuccessfully(pool, firstConnection);
        assertThat(connectionFactory.getCreateCount()).isEqualTo(1);
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
            .maxIdleTime(Duration.ZERO)  // do not evict by idle time
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

        pool.close();

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
