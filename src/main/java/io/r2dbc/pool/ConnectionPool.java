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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;
import reactor.pool.PooledRef;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Reactive Relational Database Connection Pool implementation.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 */
public class ConnectionPool implements ConnectionFactory, Disposable, Closeable, Wrapped<ConnectionFactory> {

    private final ConnectionFactory factory;

    private final Pool<Connection> connectionPool;

    private final Duration maxAcquireTime;

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
    }

    private Pool<Connection> createConnectionPool(ConnectionPoolConfiguration configuration) {

        ConnectionFactory factory = configuration.getConnectionFactory();
        Duration maxCreateConnectionTime = configuration.getMaxCreateConnectionTime();
        int initialSize = configuration.getInitialSize();
        int maxSize = configuration.getMaxSize();
        String validationQuery = configuration.getValidationQuery();
        Duration maxIdleTime = configuration.getMaxIdleTime();
        Consumer<PoolBuilder<Connection>> customizer = configuration.getCustomizer();

        // set timeout for create connection
        Mono<Connection> allocator = Mono.from(factory.create());
        if (!maxCreateConnectionTime.isZero()) {
            allocator = allocator.timeout(maxCreateConnectionTime);
        }

        PoolBuilder<Connection> builder = PoolBuilder.from(allocator)
            .destroyHandler(Connection::close)
            .sizeMax(Runtime.getRuntime().availableProcessors());

        builder.initialSize(initialSize);

        if (maxSize == -1) {
            builder.sizeUnbounded();
        } else {
            builder.sizeMax(maxSize);
        }

        if (validationQuery != null) {
            builder.releaseHandler(connection -> {
                return Flux.from(connection.createStatement(validationQuery).execute()).flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get(0)))).then();
            });
        }

        builder.evictionIdle(maxIdleTime);

        customizer.accept(builder);

        return builder.build();
    }

    @Override
    public Mono<Connection> create() {
        return Mono.defer(() -> {

            AtomicReference<PooledRef<Connection>> emitted = new AtomicReference<>();

            Mono<PooledConnection> mono = connectionPool.acquire()
                .doOnNext(emitted::set)
                .map(PooledConnection::new)
                .doOnCancel(() -> {

                    PooledRef<Connection> ref = emitted.get();
                    if (ref != null && emitted.compareAndSet(ref, null)) {
                        ref.release().subscribe();
                    }
                });

            if (!this.maxAcquireTime.isZero()) {
                mono = mono.timeout(this.maxAcquireTime);
            }
            return mono;
        });
    }

    @Override
    public void close() {
        dispose();
    }

    @Override
    public void dispose() {
        this.connectionPool.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.connectionPool.isDisposed();
    }

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        return factory.getMetadata();
    }

    @Override
    public ConnectionFactory unwrap() {
        return this.factory;
    }
}
