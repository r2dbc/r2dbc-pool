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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Lifecycle;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.pool.PoolBuilder;
import reactor.pool.PoolConfig;
import reactor.pool.PoolMetricsRecorder;
import reactor.util.annotation.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Connection pool configuration.
 * <p>Negative {@link Duration} values for timeouts are considered as no timeout.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 * @author Steffen Kreutz
 * @author Rodolfo Beletatti
 * @author Petromir Dzhunev
 * @author Gabriel Calin
 */
public final class ConnectionPoolConfiguration {

    /**
     * Constant indicating that timeout should not apply.
     */
    public static final Duration NO_TIMEOUT = Duration.ofMillis(-1);

    @Nullable
    private final Scheduler allocatorSubscribeOn;

    private final int acquireRetry;

    private final Duration backgroundEvictionInterval;

    private final ConnectionFactory connectionFactory;

    private final Clock clock;

    private final Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer;

    private final int initialSize;

    private final int maxSize;

    private final int minIdle;

    private final Duration maxAcquireTime;

    private final Duration maxCreateConnectionTime;

    private final Duration maxIdleTime;

    private final Duration maxLifeTime;

    private final Duration maxValidationTime;

    private final PoolMetricsRecorder metricsRecorder;

    @Nullable
    private final String name;

    @Nullable
    private final Function<? super Connection, ? extends Publisher<Void>> postAllocate;

    @Nullable
    private final Function<? super Connection, ? extends Publisher<Void>> preRelease;

    private final boolean registerJmx;

    private final ValidationDepth validationDepth;

    @Nullable
    private final String validationQuery;

    private ConnectionPoolConfiguration(@Nullable Scheduler allocatorSubscribeOn, int acquireRetry, @Nullable Duration backgroundEvictionInterval, ConnectionFactory connectionFactory, Clock clock, Consumer<PoolBuilder<Connection, ?
            extends PoolConfig<? extends Connection>>> customizer, int initialSize, int maxSize, int minIdle, Duration maxAcquireTime, Duration maxCreateConnectionTime, Duration maxIdleTime,
                                        Duration maxLifeTime, Duration maxValidationTime, PoolMetricsRecorder metricsRecorder, @Nullable String name,
                                        @Nullable Function<? super Connection, ? extends Publisher<Void>> postAllocate,
                                        @Nullable Function<? super Connection, ? extends Publisher<Void>> preRelease, boolean registerJmx, ValidationDepth validationDepth,
                                        @Nullable String validationQuery) {
        this.allocatorSubscribeOn = allocatorSubscribeOn;
        this.acquireRetry = acquireRetry;
        this.connectionFactory = Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null");
        this.clock = clock;
        this.customizer = customizer;
        this.initialSize = initialSize;
        this.maxSize = maxSize;
        this.minIdle = minIdle;
        this.maxIdleTime = maxIdleTime;
        this.maxAcquireTime = maxAcquireTime;
        this.maxCreateConnectionTime = maxCreateConnectionTime;
        this.maxLifeTime = maxLifeTime;
        this.maxValidationTime = maxValidationTime;
        this.metricsRecorder = metricsRecorder;
        this.name = name;
        this.registerJmx = registerJmx;
        this.postAllocate = postAllocate;
        this.preRelease = preRelease;
        this.validationDepth = validationDepth;
        this.validationQuery = validationQuery;
        this.backgroundEvictionInterval = backgroundEvictionInterval;
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @param connectionFactory the {@link ConnectionFactory} to wrap.
     * @return a new {@link Builder}
     */
    public static Builder builder(ConnectionFactory connectionFactory) {
        return new Builder().connectionFactory(connectionFactory);
    }

    /**
     * Returns a new {@link Builder}.
     *
     * @return a new {@link Builder}
     * @since 0.9
     */
    public static Builder builder() {
        return new Builder();
    }

    @Nullable
    Scheduler getAllocatorSubscribeOn() {
        return this.allocatorSubscribeOn;
    }

    int getAcquireRetry() {
        return this.acquireRetry;
    }

    Duration getBackgroundEvictionInterval() {
        return this.backgroundEvictionInterval;
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

    int getInitialSize() {
        return this.initialSize;
    }

    int getMinIdle() {
        return this.minIdle;
    }

    int getMaxSize() {
        return this.maxSize;
    }

    Duration getMaxAcquireTime() {
        return this.maxAcquireTime;
    }

    Duration getMaxCreateConnectionTime() {
        return this.maxCreateConnectionTime;
    }

    Duration getMaxIdleTime() {
        return this.maxIdleTime;
    }

    Duration getMaxLifeTime() {
        return this.maxLifeTime;
    }

    Duration getMaxValidationTime() {
        return this.maxValidationTime;
    }

    PoolMetricsRecorder getMetricsRecorder() {
        return this.metricsRecorder;
    }

    @Nullable
    String getName() {
        return this.name;
    }

    @Nullable
    Function<? super Connection, ? extends Publisher<Void>> getPostAllocate() {
        return this.postAllocate;
    }

    @Nullable
    Function<? super Connection, ? extends Publisher<Void>> getPreRelease() {
        return this.preRelease;
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

        private static final int DEFAULT_SIZE = 10;

        private @Nullable Scheduler allocatorSubscribeOn;

        private int acquireRetry = 1;

        private Duration backgroundEvictionInterval = NO_TIMEOUT;

        private ConnectionFactory connectionFactory;

        private Clock clock = Clock.systemUTC();

        private Consumer<PoolBuilder<Connection, ? extends PoolConfig<? extends Connection>>> customizer = poolBuilder -> {
        };  // no-op

        private Integer initialSize;

        private Integer maxSize;

        private int minIdle;

        private Duration maxAcquireTime = NO_TIMEOUT;  // negative value indicates no-timeout

        private Duration maxCreateConnectionTime = NO_TIMEOUT;  // negative value indicates no-timeout

        private Duration maxIdleTime = Duration.ofMinutes(30);

        private Duration maxLifeTime = NO_TIMEOUT;  // negative value indicates no-timeout

        private Duration maxValidationTime = NO_TIMEOUT;  // negative value indicates no-timeout

        private PoolMetricsRecorder metricsRecorder = new SimplePoolMetricsRecorder();

        @Nullable
        private String name;

        private boolean registerJmx;

        @Nullable
        private Function<? super Connection, ? extends Publisher<Void>> postAllocate;

        @Nullable
        private Function<? super Connection, ? extends Publisher<Void>> preRelease;

        @Nullable
        private String validationQuery;

        private ValidationDepth validationDepth = ValidationDepth.LOCAL;

        private Builder() {
        }

        /**
         * Configure {@link Scheduler} to use for allocation. Defaults to {@link Schedulers#single()}.
         * Configuring the scheduler can be relevant to coordinate thread co-location.
         *
         * @param scheduler the scheduler to use.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code scheduler} is null.
         * @see Schedulers#single()
         * @since 1.0.1
         */
        public Builder allocatorSubscribeOn(Scheduler scheduler) {
            this.allocatorSubscribeOn = Assert.requireNonNull(scheduler, "scheduler must not be null");
            return this;
        }

        /**
         * Configure the number of acquire retries if the first acquiry attempt fails.
         *
         * @param retryAttempts the number of retries. Can be zero or any positive number
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code retryAttempts} is less than zero.
         * @see Mono#retry(long)
         */
        public Builder acquireRetry(int retryAttempts) {
            if (retryAttempts < 0) {
                throw new IllegalArgumentException("retryAttempts must not be negative");
            }
            this.acquireRetry = retryAttempts;
            return this;
        }

        /**
         * Configure the background eviction {@link Duration interval} to evict idle connections while the pool isn't actively used for allocations/releases.
         *
         * @param backgroundEvictionInterval background eviction interval. {@link Duration#ZERO}, a negative or a {@code null} value results in disabling background eviction.
         * @return this {@link Builder}
         * @since 0.8.7
         */
        public Builder backgroundEvictionInterval(@Nullable Duration backgroundEvictionInterval) {
            this.backgroundEvictionInterval = applyDefault(backgroundEvictionInterval);
            return this;
        }

        /**
         * Configure the {@link Clock} used for allocation and eviction timing.
         *
         * @param clock the {@link Clock} to use.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code clock} is null.
         */
        public Builder clock(Clock clock) {
            this.clock = Assert.requireNonNull(clock, "Clock must not be null");
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
         * Configure the minimal number of idle connections. Defaults to {@code 0}.
         *
         * @param minIdle the minimal amount of idle connections in the pool, must not be negative.
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code minIdle} is negative.
         * @since 0.9.1
         */
        public Builder minIdle(int minIdle) {
            if (minIdle < 0) {
                throw new IllegalArgumentException("Minimal idle size must be greater or equal to zero");
            }
            this.minIdle = minIdle;
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
         * Configure {@link Duration timeout} for acquiring a {@link Connection} from pool. Default is no timeout.
         * <p>
         * When acquiring a {@link Connection} requires obtaining a new {@link Connection} from underlying {@link ConnectionFactory}, this timeout
         * also applies to get the new one.
         *
         * @param maxAcquireTime the maximum time to acquire connection from pool. {@link Duration#ZERO} indicates that the connection must be immediately available
         *                       otherwise acquisition fails. A negative or a {@code null} value results in not applying a timeout.
         * @return this {@link Builder}
         */
        public Builder maxAcquireTime(@Nullable Duration maxAcquireTime) {
            this.maxAcquireTime = applyDefault(maxAcquireTime);
            return this;
        }

        /**
         * Configure {@link Duration timeout} for creating a new {@link Connection} from {@link ConnectionFactory}. Default is no timeout.
         *
         * @param maxCreateConnectionTime the maximum time to create a new {@link Connection} from {@link ConnectionFactory}.
         *                                {@link Duration#ZERO} indicates immediate failure if the connection is not created immediately. A negative or a {@code null} value results in not applying
         *                                a timeout.
         * @return this {@link Builder}
         */
        public Builder maxCreateConnectionTime(@Nullable Duration maxCreateConnectionTime) {
            this.maxCreateConnectionTime = applyDefault(maxCreateConnectionTime);
            return this;
        }

        /**
         * Configure a idle {@link Duration timeout}. Defaults to 30 minutes. Configuring {@code maxIdleTime} enables background eviction using the configured idle time as interval unless
         * {@link #backgroundEvictionInterval(Duration)} is configured.
         *
         * @param maxIdleTime the maximum idle time. {@link Duration#ZERO} means immediate connection disposal. A negative or a {@code null} value results in not applying a timeout.
         * @return this {@link Builder}
         */
        public Builder maxIdleTime(@Nullable Duration maxIdleTime) {
            this.maxIdleTime = applyDefault(maxIdleTime);
            return this;
        }

        /**
         * Configure {@link Duration lifetime} of the pooled {@link Connection} in the pool. Default is no timeout.
         *
         * @param maxLifeTime the maximum lifetime of the connection in the pool,.
         *                    {@link Duration#ZERO} indicates immediate connection disposal. A negative or a {@code null} value results in not applying a timeout.
         * @return this {@link Builder}
         */
        public Builder maxLifeTime(Duration maxLifeTime) {
            this.maxLifeTime = applyDefault(maxLifeTime);
            return this;
        }

        /**
         * Configure {@link Duration timeout} for validating a {@link Connection} from pool. Default is no timeout.
         *
         * @param maxValidationTime the maximum time to validate connection from pool. {@link Duration#ZERO} indicates that the connection must be immediately validated
         *                          otherwise validation fails. A negative or a {@code null} value results in not applying a timeout.
         * @return this {@link Builder}
         * @see Connection#validate(ValidationDepth)
         * @see #validationQuery(String)
         * @see #validationDepth(ValidationDepth)
         * @since 0.9.2
         */
        public Builder maxValidationTime(Duration maxValidationTime) {
            this.maxValidationTime = applyDefault(maxValidationTime);
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
         * Configure a {@link Lifecycle#postAllocate()} callback function. This {@link Function} is called with a {@link Connection} object that was allocated from the pool before emitting it
         * through {@link ConnectionFactory#create()}.
         * The connection emission is delayed until the returned {@link Publisher} has completed. Any error signals from this publisher lead to immediate disposal of the connection.
         * If a {@link Connection} implements {@link Lifecycle}, the given function is called once {@link Lifecycle#postAllocate()} has completed.
         *
         * @param postAllocate function applied to {@link Connection} returning a publisher to perform actions before returning the connection to the caller
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code postAllocate} is {@code null}
         * @since 0.9
         */
        public Builder postAllocate(Function<? super Connection, ? extends Publisher<Void>> postAllocate) {
            this.postAllocate = Assert.requireNonNull(postAllocate, "postAllocate must not be null");
            return this;
        }

        /**
         * Configure a {@link Lifecycle#preRelease()} callback function. This {@link Function} is called with a {@link Connection} object that is about to be returned to the pool right before
         * releasing it.
         * through {@link Connection#close()}.
         * The connection release is delayed until the returned {@link Publisher} has completed. Any error signals from this publisher lead to immediate disposal of the connection instead of
         * returning it to the pool.
         * If a {@link Connection} implements {@link Lifecycle}, the given function is called before invoking {@link Lifecycle#preRelease()}.
         *
         * @param preRelease function applied to {@link Connection} returning a publisher to perform actions before releasing the connection
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code preRelease} is {@code null}
         * @since 0.9
         */
        public Builder preRelease(Function<? super Connection, ? extends Publisher<Void>> preRelease) {
            this.preRelease = Assert.requireNonNull(preRelease, "postAllocate must not be null");
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
         * @throws IllegalArgumentException if {@code validationDepth} is {@code null}
         */
        public Builder validationDepth(ValidationDepth validationDepth) {
            this.validationDepth = Assert.requireNonNull(validationDepth, "ValidationQuery must not be null");
            return this;
        }

        /**
         * Configure connection factory.
         *
         * @param connectionFactory the connection factory to connect to the db, must not be {@literal null}
         * @return this {@link Builder}
         * @throws IllegalArgumentException if {@code connectionFactory} is {@code null}
         * @since 0.9
         */
        public Builder connectionFactory(ConnectionFactory connectionFactory) {
            this.connectionFactory = Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null");
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
            applyDefaults();
            validate();
            return new ConnectionPoolConfiguration(this.allocatorSubscribeOn, this.acquireRetry, this.backgroundEvictionInterval, this.connectionFactory,
                    this.clock, this.customizer, this.initialSize, this.maxSize, this.minIdle,
                    this.maxAcquireTime, this.maxCreateConnectionTime, this.maxIdleTime, this.maxLifeTime, this.maxValidationTime,
                    this.metricsRecorder, this.name, this.postAllocate, this.preRelease, this.registerJmx,
                    this.validationDepth, this.validationQuery
            );
        }

        /**
         * Apply defaults to {@code initialSize} and {@code maxSize} if at leas one of the parameters is not set.
         */
        private void applyDefaults() {
            if (this.initialSize == null && this.maxSize == null) {
                this.initialSize = DEFAULT_SIZE;
                this.maxSize = DEFAULT_SIZE;
            } else if (this.initialSize == null) {
                this.initialSize = Math.min(DEFAULT_SIZE, this.maxSize);
            } else if (this.maxSize == null) {
                this.maxSize = Math.max(DEFAULT_SIZE, this.initialSize);
            }
        }

        private void validate() {
            Assert.requireNonNull(this.connectionFactory, "connectionFactory must not be null");

            if (this.registerJmx) {
                Assert.requireNonNull(this.name, "name must not be null when registering to JMX");
            }

            if (0 > this.initialSize) {
                throw new IllegalArgumentException("initialSize must be non-negative");
            }

            if (this.initialSize > this.maxSize) {
                throw new IllegalArgumentException("maxSize must be greater than or equal to initialSize");
            }
        }

        @Override
        public String toString() {
            return "Builder{" +
                    "allocatorSubscribeOn='" + this.allocatorSubscribeOn + '\'' +
                    ", acquireRetry='" + this.acquireRetry + '\'' +
                    ", backgroundEvictionInterval='" + this.backgroundEvictionInterval + '\'' +
                    ", connectionFactory='" + this.connectionFactory + '\'' +
                    ", clock='" + this.clock + '\'' +
                    ", initialSize='" + this.initialSize + '\'' +
                    ", minIdle='" + this.minIdle + '\'' +
                    ", maxSize='" + this.maxSize + '\'' +
                    ", maxAcquireTime='" + this.maxAcquireTime + '\'' +
                    ", maxCreateConnectionTime='" + this.maxCreateConnectionTime + '\'' +
                    ", maxIdleTime='" + this.maxIdleTime + '\'' +
                    ", maxLifeTime='" + this.maxLifeTime + '\'' +
                    ", maxValidationTime='" + this.maxValidationTime + '\'' +
                    ", metricsRecorder='" + this.metricsRecorder + '\'' +
                    ", name='" + this.name + '\'' +
                    ", postAllocate='" + this.postAllocate + '\'' +
                    ", preRelease='" + this.preRelease + '\'' +
                    ", registerJmx='" + this.registerJmx + '\'' +
                    ", validationDepth='" + this.validationDepth + '\'' +
                    ", validationQuery='" + this.validationQuery + '\'' +
                    '}';
        }

        private static Duration applyDefault(@Nullable Duration duration) {
            return duration == null ? NO_TIMEOUT : duration;
        }

    }

}
