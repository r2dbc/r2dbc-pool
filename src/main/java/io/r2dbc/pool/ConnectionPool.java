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
import io.r2dbc.spi.Wrapped;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.pool.InstrumentedPool;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolConfig;
import reactor.pool.PoolMetricsRecorder;
import reactor.pool.PooledRef;
import reactor.pool.PooledRefMetadata;
import reactor.util.Loggers;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactive Relational Database Connection Pool implementation.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 */
public class ConnectionPool implements ConnectionFactory, Disposable, Closeable, Wrapped<ConnectionFactory> {

    private final ConnectionFactory factory;

    private final InstrumentedPool<Connection> connectionPool;

    private final Duration maxAcquireTime;

    private final List<Runnable> destroyHandlers = new ArrayList<>();

    private final Optional<PoolMetrics> poolMetrics;

    private final Mono<Connection> create;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use for building the connection pool.
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public ConnectionPool(ConnectionPoolConfiguration configuration) {
        this.connectionPool = createConnectionPool(Assert.requireNonNull(configuration, "ConnectionPoolConfiguration must not be null"));
        this.factory = configuration.getConnectionFactory();
        this.maxAcquireTime = configuration.getMaxAcquireTime();
        this.poolMetrics = Optional.ofNullable(this.connectionPool.metrics()).map(PoolMetricsWrapper::new);

        if (configuration.isRegisterJmx()) {
            getMetrics().ifPresent(poolMetrics -> {
                registerToJmx(poolMetrics, configuration.getName());
            });
        }

        Function<Connection, Mono<Void>> allocateValidation = getValidationFunction(configuration);

        Mono<Connection> create = Mono.defer(() -> {

            AtomicReference<PooledRef<Connection>> emitted = new AtomicReference<>();

            Mono<Connection> mono = this.connectionPool.acquire()
                .doOnNext(emitted::set)
                .flatMap(ref -> {

                    PooledConnection connection = new PooledConnection(ref);
                    return allocateValidation.apply(connection).thenReturn((Connection) connection).onErrorResume(throwable -> {
                        return ref.invalidate().then(Mono.error(throwable));
                    });
                })
                .doOnCancel(() -> {

                    PooledRef<Connection> ref = emitted.get();
                    if (ref != null && emitted.compareAndSet(ref, null)) {
                        ref.release().subscribe();
                    }
                }).name(String.format("Connection Acquisition from [%s]", configuration.getConnectionFactory()));

            if (!this.maxAcquireTime.isZero()) {
                mono = mono.timeout(this.maxAcquireTime).onErrorMap(TimeoutException.class, e -> new R2dbcTimeoutException(String.format("Connection Acquisition timed" +
                    " out after %dms", this.maxAcquireTime.toMillis()), e));
            }
            return mono;
        });
        this.create = configuration.getAcquireRetry() > 0 ? create.retry(configuration.getAcquireRetry()) : create;
    }

    private Function<Connection, Mono<Void>> getValidationFunction(ConnectionPoolConfiguration configuration) {
        Function<Connection, Mono<Void>> allocateValidation;

        if (!this.maxAcquireTime.isZero()) {
            allocateValidation = getValidation(configuration).andThen(mono -> mono.timeout(this.maxAcquireTime).onErrorMap(TimeoutException.class, e -> new R2dbcTimeoutException(String.format(
                "Validation timed out after %dms", this.maxAcquireTime.toMillis()), e)));
        } else {
            allocateValidation = getValidation(configuration);
        }

        return allocateValidation;
    }

    private Function<Connection, Mono<Void>> getValidation(ConnectionPoolConfiguration configuration) {

        String validationQuery = configuration.getValidationQuery();
        if (validationQuery != null && !validationQuery.isEmpty()) {
            return connection -> Validation.validate(connection, validationQuery);
        }

        return connection -> Validation.validate(connection, configuration.getValidationDepth());
    }

    /**
     * Warms up the {@link ConnectionPool}, if needed. This instructs the pool to check for a minimum size and allocate
     * necessary connections when the minimum is not reached.
     *
     * @return a cold {@link Mono} that triggers resource warmup and emits the number of warmed up resources.
     */
    public Mono<Integer> warmup() {
        return this.connectionPool.warmup();
    }

    private InstrumentedPool<Connection> createConnectionPool(ConnectionPoolConfiguration configuration) {

        ConnectionFactory factory = configuration.getConnectionFactory();
        Duration maxCreateConnectionTime = configuration.getMaxCreateConnectionTime();
        int initialSize = configuration.getInitialSize();
        int maxSize = configuration.getMaxSize();
        Duration maxIdleTime = configuration.getMaxIdleTime();
        Duration maxLifeTime = configuration.getMaxLifeTime();
        Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer = configuration.getCustomizer();
        PoolMetricsRecorder metricsRecorder = configuration.getMetricsRecorder();

        if (factory instanceof ConnectionPool) {
            Loggers.getLogger(ConnectionPool.class).warn(String.format("Creating ConnectionPool using another ConnectionPool [%s] as ConnectionFactory", factory));
        }

        // set timeout for create connection
        Mono<Connection> allocator = Mono.<Connection>from(factory.create()).name("Connection Allocation");
        if (!maxCreateConnectionTime.isZero()) {
            allocator = allocator.timeout(maxCreateConnectionTime);
        }

        // Create eviction predicate that checks maxIdleTime and maxLifeTime.
        // This is because "PoolBuilder#evictionIdle()" and "PoolBuilder#evictionPredicate()" cannot be used together in
        // current implementation. (https://github.com/reactor/reactor-pool/issues/33)
        // To workaround the issue, here defines an evictionPredicate that performs both maxIdleTime and maxLifeTime check.
        BiPredicate<Connection, PooledRefMetadata> evictionPredicate = (connection, metadata) -> {
            long maxIdleTimeMills = maxIdleTime.toMillis();
            long maxLifeTimeMillis = maxLifeTime.toMillis();
            boolean isIdleTimeExceeded = maxIdleTimeMills != 0 && metadata.idleTime() >= maxIdleTimeMills;
            boolean isLifeTimeExceeded = maxLifeTimeMillis != 0 && metadata.lifeTime() >= maxLifeTimeMillis;
            return isIdleTimeExceeded || isLifeTimeExceeded;
        };

        PoolBuilder<Connection, PoolConfig<Connection>> builder = PoolBuilder.from(allocator)
            .clock(configuration.getClock())
            .metricsRecorder(metricsRecorder)
            .evictionPredicate(evictionPredicate)
            .destroyHandler(Connection::close)
            .sizeBetween(0, Runtime.getRuntime().availableProcessors());

        if (maxSize == -1 || initialSize > 0) {
            builder.sizeBetween(Math.max(0, initialSize), maxSize == -1 ? Integer.MAX_VALUE : maxSize);
        } else {
            builder.sizeBetween(initialSize, maxSize);
        }

        customizer.accept(builder);

        return builder.fifo();
    }

    @Override
    public Mono<Connection> create() {
        return this.create;
    }

    @Override
    public void close() {
        dispose();
    }

    @Override
    public void dispose() {
        disposeLater().block();
    }

    /**
     * Dispose this {@link ConnectionPool} in non-blocking flow.
     * <p>
     * When multiple errors occurred during dispose flow, they are added as
     * suppressed errors onto the first error.
     *
     * @return a Mono triggering the shutdown of the pool once subscribed.
     */
    public Mono<Void> disposeLater() {

        if (isDisposed()) {
            return Mono.empty();
        }

        List<Throwable> errors = new ArrayList<>();
        return Flux.fromIterable(this.destroyHandlers)
            .flatMap(Mono::fromRunnable)
            .concatWith(this.connectionPool.disposeLater())
            .onErrorContinue((throwable, o) -> {
                errors.add(throwable);
            })
            .then(Mono.defer(() -> {
                if (errors.isEmpty()) {
                    return Mono.empty();
                }

                Throwable rootError = errors.get(0);
                if (errors.size() == 1) {
                    return Mono.error(rootError);
                }

                errors.subList(1, errors.size()).forEach(rootError::addSuppressed);

                return Mono.error(rootError);
            }));
    }

    @Override
    public boolean isDisposed() {
        return this.connectionPool.isDisposed();
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return this.factory.getMetadata();
    }

    @Override
    public ConnectionFactory unwrap() {
        return this.factory;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append("[");
        sb.append(this.factory.getMetadata().getName());
        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns {@link PoolMetrics} if available.
     *
     * @return the optional pool metrics.
     */
    public Optional<PoolMetrics> getMetrics() {
        return this.poolMetrics;
    }

    private void registerToJmx(PoolMetrics poolMetrics, String name) {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

        try {
            ObjectName jmxName = getPoolObjectName(name);
            mBeanServer.registerMBean(new ConnectionPoolMXBeanImpl(poolMetrics), jmxName);

            // add a destroy handler to unregister the mbean
            this.destroyHandlers.add(() -> {
                try {
                    mBeanServer.unregisterMBean(jmxName);
                } catch (JMException e) {
                    throw new ConnectionPoolException("Failed to unregister from JMX", e);
                }
            });
        } catch (JMException e) {
            throw new ConnectionPoolException("Failed to register to JMX", e);
        }
    }

    /**
     * Construct JMX {@link ObjectName}.
     *
     * @param name connection pool name
     * @return JMX {@link ObjectName}
     * @throws MalformedObjectNameException when invalid objectname is constructed
     */
    protected ObjectName getPoolObjectName(String name) throws MalformedObjectNameException {
        Hashtable<String, String> prop = new Hashtable<>();
        prop.put("type", ConnectionPool.class.getSimpleName());
        prop.put("name", name);
        return new ObjectName(ConnectionPoolMXBean.DOMAIN, prop);
    }

    private class PoolMetricsWrapper implements PoolMetrics {

        private final InstrumentedPool.PoolMetrics delegate;

        PoolMetricsWrapper(InstrumentedPool.PoolMetrics delegate) {
            this.delegate = delegate;
        }

        @Override
        public int acquiredSize() {
            return this.delegate.acquiredSize();
        }

        @Override
        public int allocatedSize() {
            return this.delegate.allocatedSize();
        }

        @Override
        public int idleSize() {
            return this.delegate.idleSize();
        }

        @Override
        public int pendingAcquireSize() {
            return this.delegate.pendingAcquireSize();
        }

        @Override
        public int getMaxAllocatedSize() {
            return this.delegate.getMaxAllocatedSize();
        }

        @Override
        public int getMaxPendingAcquireSize() {
            return this.delegate.getMaxPendingAcquireSize();
        }
    }

    private class ConnectionPoolMXBeanImpl implements ConnectionPoolMXBean {

        private final PoolMetrics poolMetrics;

        ConnectionPoolMXBeanImpl(PoolMetrics poolMetrics) {
            this.poolMetrics = poolMetrics;
        }

        @Override
        public int getAcquiredSize() {
            return this.poolMetrics.acquiredSize();
        }

        @Override
        public int getAllocatedSize() {
            return this.poolMetrics.allocatedSize();
        }

        @Override
        public int getIdleSize() {
            return this.poolMetrics.idleSize();
        }

        @Override
        public int getPendingAcquireSize() {
            return this.poolMetrics.pendingAcquireSize();
        }

        @Override
        public int getMaxAllocatedSize() {
            return this.poolMetrics.getMaxAllocatedSize();
        }

        @Override
        public int getMaxPendingAcquireSize() {
            return this.poolMetrics.getMaxPendingAcquireSize();
        }
    }
}
