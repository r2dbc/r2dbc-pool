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
import io.r2dbc.spi.ValidationDepth;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolConfig;
import reactor.pool.PoolMetricsRecorder;
import reactor.util.annotation.Nullable;

import java.time.Clock;
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

    private final Clock clock;

    private final Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer;

    private final int initialSize;

    private final int maxSize;

    private final Duration maxIdleTime;

    private final Duration maxCreateConnectionTime;

    private final Duration maxAcquireTime;

    private final Duration maxLifeTime;

    private final PoolMetricsRecorder metricsRecorder;

    @Nullable
    private final String name;

    private final boolean registerJmx;

    private final ValidationDepth validationDepth;

    @Nullable
    private final String validationQuery;

    public ConnectionPoolConfiguration(ConnectionFactory connectionFactory, Clock clock, Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer,
                                       int initialSize, int maxSize, Duration maxIdleTime, Duration maxCreateConnectionTime, Duration maxAcquireTime, Duration maxLifeTime,
                                       PoolMetricsRecorder metricsRecorder, @Nullable String name, boolean registerJmx, ValidationDepth validationDepth, @Nullable String validationQuery) {
        this.connectionFactory = Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null");
        this.clock = clock;
        this.customizer = customizer;
        this.initialSize = initialSize;
        this.maxSize = maxSize;
        this.maxIdleTime = maxIdleTime;
        this.maxCreateConnectionTime = maxCreateConnectionTime;
        this.maxAcquireTime = maxAcquireTime;
        this.maxLifeTime = maxLifeTime;
        this.metricsRecorder = metricsRecorder;
        this.name = name;
        this.registerJmx = registerJmx;
        this.validationDepth = validationDepth;
        this.validationQuery = validationQuery;
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
        return this.connectionFactory;
    }

    Clock getClock() {
        return this.clock;
    }

    Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> getCustomizer() {
        return this.customizer;
    }

    Duration getMaxIdleTime() {
        return this.maxIdleTime;
    }

    int getInitialSize() {
        return this.initialSize;
    }

    int getMaxSize() {
        return this.maxSize;
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

    PoolMetricsRecorder getMetricsRecorder() {
        return this.metricsRecorder;
    }

    @Nullable
    String getName() {
        return this.name;
    }

    boolean isRegisterJmx() {
        return this.registerJmx;
    }

    ValidationDepth getValidationDepth() {
        return this.validationDepth;
    }

    @Nullable
    String getValidationQuery() {
        return this.validationQuery;
    }

    /**
     * A builder for {@link ConnectionPoolConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private final ConnectionFactory connectionFactory;

        private Clock clock = Clock.systemUTC();

        private Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer = poolBuilder -> {
        };  // no-op

        private int initialSize = 10;

        private int maxSize = 10;

        private Duration maxIdleTime = Duration.ofMinutes(30);

        private Duration maxCreateConnectionTime = Duration.ZERO;  // ZERO indicates no-timeout

        private Duration maxAcquireTime = Duration.ZERO;  // ZERO indicates no-timeout

        private Duration maxLifeTime = Duration.ZERO;  // ZERO indicates no-lifetime

        private PoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        @Nullable
        private String name;

        private boolean registerJmx;

        @Nullable
        private String validationQuery;

        private ValidationDepth validationDepth = ValidationDepth.LOCAL;

        private Builder(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        /**
         * Configure the {@link Clock} used for allocation and eviction timing.
         *
         * @param clock the {@link Clock} to use.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code clock} is null.
         */
        public Builder clock(Clock clock) {
            if (clock == null) {
                throw new IllegalArgumentException("Clock must not be null");
            }
            this.clock = clock;
            return this;
        }

        /**
         * Configure a customizer for {@link PoolBuilder} that constructs the {@link Connection} pool.
         *
         * @param customizer customizer for {@link PoolBuilder} that creates the {@link Connection} pool, must not be {@code null}.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code customizer} is {@code null}
         */
        public Builder customizer(Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer) {
            this.customizer = Assert.requireNonNull(customizer, "PoolBuilder customizer must not be null");
            return this;
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
         * Configure validation depth for {@link Connection#validate(ValidationDepth) connection validation}.
         *
         * @param validationDepth the depth of validation, must not be {@literal null}
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code validationQuery} is {@code null}
         */
        public Builder validationDepth(ValidationDepth validationDepth) {
            this.validationDepth = Assert.requireNonNull(validationDepth, "ValidationQuery must not be null");
            return this;
        }

        /**
         * Configure a validation query. When a validation query is used, then {@link Connection#validate(ValidationDepth)} is not used.
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
         * Returns a configured {@link ConnectionPoolConfiguration}.
         *
         * @return a configured {@link ConnectionPoolConfiguration}
         * @throws IllegalArgumentException if {@code registerJmx} is {@code true} AND {@code name} is {@code null}
         */
        public ConnectionPoolConfiguration build() {
            validate();
            return new ConnectionPoolConfiguration(this.connectionFactory, this.clock, this.customizer, this.initialSize, this.maxSize, this.maxIdleTime, this.maxCreateConnectionTime,
                this.maxAcquireTime, this.maxLifeTime, this.metricsRecorder, this.name, this.registerJmx, this.validationDepth, this.validationQuery
            );
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
                ", clock='" + this.clock + '\'' +
                ", initialSize='" + this.initialSize + '\'' +
                ", maxSize='" + this.maxSize + '\'' +
                ", maxIdleTime='" + this.maxIdleTime + '\'' +
                ", maxCreateConnectionTime='" + this.maxCreateConnectionTime + '\'' +
                ", maxAcquireTime='" + this.maxAcquireTime + '\'' +
                ", maxLifeTime='" + this.maxLifeTime + '\'' +
                ", metricsRecorder='" + this.metricsRecorder + '\'' +
                ", name='" + this.name + '\'' +
                ", registerJmx='" + this.registerJmx + '\'' +
                ", validationDepth='" + this.validationDepth + '\'' +
                ", validationQuery='" + this.validationQuery + '\'' +
                '}';
        }
    }
}
