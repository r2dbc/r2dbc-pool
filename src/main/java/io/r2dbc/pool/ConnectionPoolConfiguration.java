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
import reactor.pool.PoolBuilder;
import reactor.pool.PoolMetricsRecorder;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Connection pool configuration.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 */
public final class ConnectionPoolConfiguration {

    private final ConnectionFactory connectionFactory;

    private final Duration maxIdleTime;

    private final Duration maxCreateConnectionTime;

    private final Duration maxAcquireTime;

    private final Duration maxLifeTime;

    private final int initialSize;

    private final int maxSize;

    @Nullable
    private final String validationQuery;

    private final Consumer<PoolBuilder<Connection>> customizer;

    private final PoolMetricsRecorder metricsRecorder;

    private final boolean registerJmx;

    private final String name;

    private ConnectionPoolConfiguration(ConnectionFactory connectionFactory, @Nullable String name, Duration maxIdleTime,
                                        int initialSize, int maxSize, @Nullable String validationQuery, Duration maxCreateConnectionTime,
                                        Duration maxAcquireTime, Duration maxLifeTime, Consumer<PoolBuilder<Connection>> customizer,
                                        PoolMetricsRecorder metricsRecorder, boolean registerJmx) {
        this.connectionFactory = Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null");
        this.name = name;
        this.initialSize = initialSize;
        this.maxSize = maxSize;
        this.maxIdleTime = maxIdleTime;
        this.validationQuery = validationQuery;
        this.maxCreateConnectionTime = maxCreateConnectionTime;
        this.maxAcquireTime = maxAcquireTime;
        this.maxLifeTime = maxLifeTime;
        this.customizer = customizer;
        this.metricsRecorder = metricsRecorder;
        this.registerJmx = registerJmx;
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @param connectionFactory the {@link ConnectionFactory} to wrap.
     * @return a new {@link Builder}
     */
    public static Builder builder(ConnectionFactory connectionFactory) {
        return new Builder(Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null"));
    }

    ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    @Nullable
    String getName() {
        return name;
    }

    Duration getMaxIdleTime() {
        return maxIdleTime;
    }


    public int getInitialSize() {
        return initialSize;
    }

    int getMaxSize() {
        return maxSize;
    }

    @Nullable
    String getValidationQuery() {
        return validationQuery;
    }

    Duration getMaxCreateConnectionTime() {
        return this.maxCreateConnectionTime;
    }

    Duration getMaxAcquireTime() {
        return this.maxAcquireTime;
    }

    Duration getMaxLifeTime() {
        return this.maxLifeTime;
    }

    Consumer<PoolBuilder<Connection>> getCustomizer() {
        return this.customizer;
    }

    PoolMetricsRecorder getMetricsRecorder() {
        return this.metricsRecorder;
    }

    boolean isRegisterJmx() {
        return this.registerJmx;
    }

    /**
     * A builder for {@link ConnectionPoolConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private final ConnectionFactory connectionFactory;

        private int initialSize = 10;

        private int maxSize = 10;

        private Duration maxIdleTime = Duration.ofMinutes(30);

        private Duration maxCreateConnectionTime = Duration.ZERO;  // ZERO indicates no-timeout

        private Duration maxAcquireTime = Duration.ZERO;  // ZERO indicates no-timeout

        private Duration maxLifeTime = Duration.ZERO;  // ZERO indicates no-lifetime

        private Consumer<PoolBuilder<Connection>> customizer = poolBuilder -> {
        };  // no-op

        @Nullable
        private String validationQuery;

        private PoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        private boolean registerJmx;

        @Nullable
        private String name;

        private Builder(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        /**
         * Configure the initial connection pool size. Defaults to {@code 10}.
         *
         * @param initialSize the initial pool size, must be equal or greater than zero.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxSize} is negative or zero.
         */
        public Builder initialSize(int initialSize) {
            if (initialSize < 0) {
                throw new IllegalArgumentException("Initial pool size must be equal greater zero");
            }
            this.initialSize = initialSize;
            return this;
        }

        /**
         * Configure the maximal connection pool size. Defaults to {@code 10}.
         *
         * @param maxSize the maximal pool size, must be greater than zero.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxSize} is negative or zero.
         */
        public Builder maxSize(int maxSize) {
            if (maxSize < 1) {
                throw new IllegalArgumentException("Maximal pool size must be greater zero");
            }
            this.maxSize = maxSize;
            return this;
        }

        /**
         * Configure a idle {@link Duration timeout}. Defaults to 30 minutes.
         *
         * @param maxIdleTime the maximum idle time, must not be {@code null} and must not be negative. {@link Duration#ZERO} means no idle timeout.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxIdleTime} is {@code null} or negative value.
         */
        public Builder maxIdleTime(Duration maxIdleTime) {
            Assert.requireNonNull(maxIdleTime, "maxIdleTime must not be null");
            if (maxIdleTime.isNegative()) {
                throw new IllegalArgumentException("maxIdleTime must not be negative");
            }
            this.maxIdleTime = maxIdleTime;
            return this;
        }

        /**
         * Configure {@link Duration timeout} for creating a new {@link Connection} from {@link ConnectionFactory}. Default is no timeout.
         *
         * @param maxCreateConnectionTime the maximum time to create a new {@link Connection} from {@link ConnectionFactory}, must not be {@code null} and must not be negative.
         *                                {@link Duration#ZERO} indicates no timeout.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxCreateConnectionTime} is {@code null} or negative.
         */
        public Builder maxCreateConnectionTime(Duration maxCreateConnectionTime) {
            Assert.requireNonNull(maxCreateConnectionTime, "maxCreateConnectionTime must not be null");
            if (maxCreateConnectionTime.isNegative()) {
                throw new IllegalArgumentException("maxCreateConnectionTime must not be negative");
            }
            this.maxCreateConnectionTime = maxCreateConnectionTime;
            return this;
        }

        /**
         * Configure {@link Duration timeout} for acquiring a {@link Connection} from pool. Default is no timeout.
         * <p>
         * When acquiring a {@link Connection} requires obtaining a new {@link Connection} from underlying {@link ConnectionFactory}, this timeout
         * also applies to get the new one.
         *
         * @param maxAcquireTime the maximum time to acquire connection from pool, must not be {@code null} and must not be negative.
         *                       {@link Duration#ZERO} indicates no timeout.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxAcquireTime} is negative.
         */
        public Builder maxAcquireTime(Duration maxAcquireTime) {
            Assert.requireNonNull(maxAcquireTime, "maxAcquireTime must not be null");
            if (maxAcquireTime.isNegative()) {
                throw new IllegalArgumentException("maxAcquireTime must not be negative");
            }
            this.maxAcquireTime = maxAcquireTime;
            return this;
        }

        /**
         * Configure {@link Duration lifetime} of the pooled {@link Connection} in the pool. Default is no timeout.
         *
         * @param maxLifeTime the maximum lifetime of the connection in the pool, must not be {@code null} and must not be negative.
         *                    {@link Duration#ZERO} indicates no lifetime.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code maxLifeTime} is negative.
         */
        public Builder maxLifeTime(Duration maxLifeTime) {
            Assert.requireNonNull(maxLifeTime, "maxLifeTime must not be null");
            if (maxLifeTime.isNegative()) {
                throw new IllegalArgumentException("maxLifeTime must not be negative");
            }
            this.maxLifeTime = maxLifeTime;
            return this;
        }

        /**
         * Configure a validation query.
         *
         * @param validationQuery the validation query to run before returning a {@link Connection} from the pool, must not be {@code null}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code validationQuery} is {@code null}
         */
        public Builder validationQuery(String validationQuery) {
            this.validationQuery = Assert.requireNonNull(validationQuery, "ValidationQuery must not be null");
            return this;
        }

        /**
         * Configure a customizer for {@link PoolBuilder} that constructs the {@link Connection} pool.
         *
         * @param customizer customizer for {@link PoolBuilder} that creates the {@link Connection} pool, must not be {@code null}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code customizer} is {@code null}
         */
        public Builder customizer(Consumer<PoolBuilder<Connection>> customizer) {
            this.customizer = Assert.requireNonNull(customizer, "PoolBuilder customizer must not be null");
            return this;
        }

        /**
         * Configure {@link PoolMetricsRecorder} to calculate elapsed time and instrumentation data
         *
         * @param recorder the {@link PoolMetricsRecorder}
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code recorder} is {@code null}
         */
        public Builder metricsRecorder(PoolMetricsRecorder recorder) {
            this.metricsRecorder = Assert.requireNonNull(recorder, "PoolMetricsRecorder must not be null");
            return this;
        }
        /**
         * Configure whether to register to JMX. Defaults to {@code false}.
         *
         * @param registerJmx register the pool to JMX
         * @return this {@link Builder}
         */
        public Builder registerJmx(boolean registerJmx) {
            this.registerJmx = registerJmx;
            return this;
        }
        /**
         * Configure the name of the connection pool.
         *
         * @param name pool name
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code name} is {@code null}
         */
        public Builder name(String name) {
            this.name = Assert.requireNonNull(name, "name must not be null");
            return this;
        }

        /**
         * Returns a configured {@link ConnectionPoolConfiguration}.
         *
         * @return a configured {@link ConnectionPoolConfiguration}
         * @throws IllegalArgumentException if {@code registerJmx} is {@code true} AND {@code name} is {@code null}
         */
        public ConnectionPoolConfiguration build() {
            validate();
            return new ConnectionPoolConfiguration(this.connectionFactory, this.name, this.maxIdleTime,
                this.initialSize, this.maxSize, this.validationQuery, this.maxCreateConnectionTime,
                this.maxAcquireTime, this.maxLifeTime, this.customizer, this.metricsRecorder, this.registerJmx);
        }

        private void validate() {
            if (this.registerJmx) {
                Assert.requireNonNull(this.name, "name must not be null when registering to JMX");
            }
        }

        @Override
        public String toString() {
            return "Builder{" +
                "connectionFactory='" + this.connectionFactory + '\'' +
                ", name='" + this.name + '\'' +
                ", maxIdleTime='" + this.maxIdleTime + '\'' +
                ", maxCreateConnectionTime='" + this.maxCreateConnectionTime + '\'' +
                ", maxAcquireTime='" + this.maxAcquireTime + '\'' +
                ", maxLifeTime='" + this.maxLifeTime + '\'' +
                ", initialSize='" + this.initialSize + '\'' +
                ", maxSize='" + this.maxSize + '\'' +
                ", validationQuery='" + this.validationQuery + '\'' +
                ", metricsRecorder='" + this.metricsRecorder + '\'' +
                ", registerJmx='" + this.registerJmx + '\'' +
                '}';
        }
    }
}
