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
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.*;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reactive Relational Database Connection Pool implementation.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 * @author Petromir Dzhunev
 * @author Gabriel Calin
 */
public class ConnectionPool implements ConnectionFactory, Disposable, Closeable, Wrapped<ConnectionFactory> {

    private static final Logger logger = Loggers.getLogger(ConnectionPool.class);

    private static final String HOOK_ON_DROPPED = "reactor.onNextDropped.local";

    private final ConnectionFactory factory;

    private final InstrumentedPool<Connection> connectionPool;

    private final Duration maxAcquireTime;

    private final Duration maxValidationTime;

    private final List<Runnable> destroyHandlers = new ArrayList<>();

    private final Optional<PoolMetrics> poolMetrics;

    private final Mono<Connection> create;

    @Nullable
    private final Function<? super Connection, ? extends Publisher<Void>> preRelease;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use for building the connection pool.
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public ConnectionPool(ConnectionPoolConfiguration configuration) {
        this.connectionPool = createConnectionPool(Assert.requireNonNull(configuration, "ConnectionPoolConfiguration must not be null"));
        this.factory = configuration.getConnectionFactory();
        this.maxAcquireTime = configuration.getMaxAcquireTime();
        this.maxValidationTime = configuration.getMaxValidationTime();
        this.poolMetrics = Optional.ofNullable(this.connectionPool.metrics()).map(PoolMetricsWrapper::new);
        this.preRelease = configuration.getPreRelease();

        if (configuration.isRegisterJmx()) {
            getMetrics().ifPresent(poolMetrics -> {
                registerToJmx(poolMetrics, configuration.getName());
            });
        }

        String acqName = String.format("Connection acquisition from [%s]", configuration.getConnectionFactory());
        String timeoutMessage = String.format("Connection acquisition timed out after %dms", this.maxAcquireTime.toMillis());

        Function<Connection, Mono<Void>> allocateValidation = getValidationFunction(configuration);

        Mono<Connection> create = Mono.defer(() -> {

            Mono<Connection> mono = this.connectionPool.acquire()
                    .flatMap(ref -> {

                        Connection connection = ref.poolable();
                        Scheduler scheduler = null;
                        Mono<Connection> conn;

                        if (connection instanceof Wrapped<?>) {
                            scheduler = findScheduler((Wrapped<?>) connection);
                        }

                        if (scheduler != null) {
                            conn = Mono.just(connection).publishOn(scheduler).flatMap(it -> {
                                return prepareConnection(configuration, ref, connection, allocateValidation);
                            });
                        } else {
                            conn = prepareConnection(configuration, ref, connection, allocateValidation);
                        }

                        return Operators.discardOnCancel(conn, () -> {
                            ref.release().subscribe();
                            return false;
                        });
                    })
                    .name(acqName);

            if (!this.maxAcquireTime.isNegative()) {

                Consumer<Object> disposeConnection = dropped -> {
                    if (dropped instanceof PooledConnection) {
                        Mono.from(((PooledConnection) dropped).close()).subscribe();
                    }
                };

                mono = mono.timeout(this.maxAcquireTime).contextWrite(context -> {

                    Consumer<Object> onNextDropped = context.getOrEmpty(HOOK_ON_DROPPED).map(it -> (Consumer<Object>) it).map(it -> {

                        return (Consumer<Object>) dropped -> {
                            disposeConnection.accept(dropped);
                            it.accept(dropped);
                        };
                    }).orElse(disposeConnection);

                    return context.put(HOOK_ON_DROPPED, onNextDropped);
                }).onErrorMap(TimeoutException.class, e -> new R2dbcTimeoutException(timeoutMessage, e));
            }
            return mono;
        });
        this.create = configuration.getAcquireRetry() > 0 ? create.retry(configuration.getAcquireRetry()) : create;
    }

    @Nullable
    @SuppressWarnings("DataFlowIssue")
    private static Scheduler findScheduler(Wrapped<?> connection) {

        Object unwrapped = connection.unwrap(Scheduler.class);

        if (!(unwrapped instanceof Scheduler)) {
            unwrapped = connection.unwrap(Executor.class);
        }

        if (unwrapped instanceof Executor) {
            unwrapped = Schedulers.fromExecutor((Executor) unwrapped);
        }

        if (unwrapped instanceof Scheduler) {
            return (Scheduler) unwrapped;
        }

        return null;
    }

    private Mono<Connection> prepareConnection(ConnectionPoolConfiguration configuration, PooledRef<Connection> ref, Connection connection, Function<Connection, Mono<Void>> allocateValidation) {

        Mono<Void> prepare = null;

        if (logger.isDebugEnabled()) {
            logger.debug("Obtaining new connection from the pool");
        }


        if (connection instanceof Lifecycle) {
            prepare = Mono.from(((Lifecycle) connection).postAllocate());
        }

        if (configuration.getPostAllocate() != null) {

            Mono<Void> postAllocate = Mono.defer(() -> Mono.from(configuration.getPostAllocate().apply(connection)));
            prepare = prepare == null ? postAllocate : prepare.then(postAllocate);
        }

        PooledConnection pooledConnection = new PooledConnection(ref, this.preRelease);
        Mono<Connection> conn;
        if (prepare == null) {
            conn = getValidConnection(allocateValidation, pooledConnection);
        } else {
            conn = prepare.then(getValidConnection(allocateValidation, pooledConnection));
        }

        conn = conn.onErrorResume(throwable -> ref.invalidate().then(Mono.error(throwable)));
        return conn;
    }

    private Mono<Connection> getValidConnection(Function<Connection, Mono<Void>> allocateValidation, Connection connection) {
        return allocateValidation.apply(connection).thenReturn(connection);
    }

    private Function<Connection, Mono<Void>> getValidationFunction(ConnectionPoolConfiguration configuration) {

        Function<Connection, Mono<Void>> validation = getValidation(configuration);

        if (!this.maxValidationTime.isNegative()) {
            String timeoutMessage = String.format("Validation timed out after %dms", this.maxValidationTime.toMillis());
            return validation.andThen(mono -> mono.timeout(this.maxValidationTime).onErrorMap(TimeoutException.class, e -> new R2dbcTimeoutException(timeoutMessage, e)));
        }

        return validation;
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

    @SuppressWarnings("unchecked")
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

        if (configuration.getAllocatorSubscribeOn() == null) {
            allocator = allocator.subscribeOn(Schedulers.single());
        }

        if (!maxCreateConnectionTime.isNegative()) {

            Consumer<Object> disposeConnection = dropped -> {
                if (dropped instanceof Connection) {
                    Mono.from(((Connection) dropped).close()).subscribe();
                }
            };

            allocator = allocator.timeout(maxCreateConnectionTime).contextWrite(context -> {

                Consumer<Object> onNextDropped = context.getOrEmpty(HOOK_ON_DROPPED).map(it -> (Consumer<Object>) it).map(it -> {

                    return (Consumer<Object>) dropped -> {
                        disposeConnection.accept(dropped);
                        it.accept(dropped);
                    };
                }).orElse(disposeConnection);

                return context.put(HOOK_ON_DROPPED, onNextDropped);
            });
        }

        // Create eviction predicate that checks maxIdleTime and maxLifeTime.
        // This is because "PoolBuilder#evictionIdle()" and "PoolBuilder#evictionPredicate()" cannot be used together in
        // current implementation. (https://github.com/reactor/reactor-pool/issues/33)
        // To workaround the issue, here defines an evictionPredicate that performs both maxIdleTime and maxLifeTime check.
        BiPredicate<Connection, PooledRefMetadata> evictionPredicate = (connection, metadata) -> {
            if (maxIdleTime.isZero() || maxLifeTime.isZero()) {
                // evict immediately
                return true;
            }

            boolean isIdleTimeExceeded = !maxIdleTime.isNegative() && metadata.idleTime() >= maxIdleTime.toMillis();
            boolean isLifeTimeExceeded = !maxLifeTime.isNegative() && metadata.lifeTime() >= maxLifeTime.toMillis();
            return isIdleTimeExceeded || isLifeTimeExceeded;
        };

        PoolBuilder<Connection, PoolConfig<Connection>> builder = PoolBuilder.from(allocator)
                .clock(configuration.getClock())
                .metricsRecorder(metricsRecorder)
                .evictionPredicate(evictionPredicate)
                .destroyHandler(Connection::close)
                .idleResourceReuseMruOrder(); // MRU to support eviction of idle

        if (maxSize == -1 || initialSize > 0) {
            builder.sizeBetween(Math.max(configuration.getMinIdle(), initialSize), maxSize == -1 ? Integer.MAX_VALUE : maxSize);
        } else {
            builder.sizeBetween(Math.max(configuration.getMinIdle(), initialSize), maxSize);
        }

        Duration backgroundEvictionInterval = configuration.getBackgroundEvictionInterval();

        if (!backgroundEvictionInterval.isZero()) {
            if (!backgroundEvictionInterval.isNegative()) {
                builder.evictInBackground(backgroundEvictionInterval);
            } else if (!maxIdleTime.isNegative()) {
                builder.evictInBackground(maxIdleTime);
            }
        }

        customizer.accept(builder);

        return builder.buildPool();
    }

    @Override
    public Mono<Connection> create() {
        return this.create;
    }

    @Override
    public Mono<Void> close() {
        return disposeLater();
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
