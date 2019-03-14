/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.publisher.Mono;
import reactor.pool.Pool;
import reactor.pool.PoolBuilder;
import reactor.pool.PooledRef;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Reactive Relational Database Connection Pool implementation.
 */
public class ConnectionPool implements ConnectionFactory, Disposable, Wrapped<ConnectionFactory> {

    private final ConnectionFactory factory;

    private final Pool<Connection> connectionPool;

    /**
     * Creates a new connection factory.
     *
     * @param configuration the configuration to use connections
     * @throws IllegalArgumentException if {@code configuration} is {@code null}
     */
    public ConnectionPool(ConnectionPoolConfiguration configuration) {
        this(Assert.requireNonNull(configuration.getConnectionFactory(), "ConnectionPoolConfiguration must not be null"), configuration.getCustomizer());
    }

    /**
     * Creates a new connection factory.
     *
     * @param factory               the {@link ConnectionFactory} that creates actual database connections
     * @param poolBuilderCustomizer {@link Consumer customizer} for the {@link PoolBuilder pool builder}.
     * @throws IllegalArgumentException if {@code factory} or {@code poolBuilderCustomizer} is {@code null}
     */
    public ConnectionPool(ConnectionFactory factory, Consumer<PoolBuilder<Connection>> poolBuilderCustomizer) {

        Assert.requireNonNull(factory, "ConnectionFactory must not be null");
        Assert.requireNonNull(poolBuilderCustomizer, "poolBuilderCustomizer must not be null");

        PoolBuilder<Connection> builder = PoolBuilder.<Connection>from(factory.create())
            .destroyHandler(Connection::close)
            .sizeMax(Runtime.getRuntime().availableProcessors());

        poolBuilderCustomizer.accept(builder);

        this.factory = factory;
        this.connectionPool = builder.build();
    }

    @Override
    public Mono<Connection> create() {
        return Mono.defer(() -> {

            AtomicReference<PooledRef<Connection>> emitted = new AtomicReference<>();
            return connectionPool.acquire().doOnNext(emitted::set).map(PooledConnection::new).doOnCancel(() -> {

                PooledRef<Connection> ref = emitted.get();
                if (ref != null && emitted.compareAndSet(ref, null)) {
                    ref.release().subscribe();
                }
            });
        });
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
