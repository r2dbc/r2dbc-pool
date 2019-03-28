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
import reactor.core.publisher.Flux;
import reactor.pool.PoolBuilder;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Connection pool configuration.
 *
 * @author Mark Paluch
 */
public final class ConnectionPoolConfiguration {

    private final ConnectionFactory connectionFactory;

    private final Duration maxIdleTime;

    private final int maxSize;

    @Nullable
    private final String validationQuery;

    private ConnectionPoolConfiguration(ConnectionFactory connectionFactory, Duration maxIdleTime, int maxSize, @Nullable String validationQuery) {
        this.connectionFactory = Assert.requireNonNull(connectionFactory, "ConnectionFactory must not be null");
        this.maxSize = maxSize;
        this.maxIdleTime = maxIdleTime;
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
        return connectionFactory;
    }

    Duration getMaxIdleTime() {
        return maxIdleTime;
    }

    int getMaxSize() {
        return maxSize;
    }

    @Nullable
    String getValidationQuery() {
        return validationQuery;
    }

    Consumer<PoolBuilder<Connection>> getCustomizer() {
        return builder -> {

            if (getMaxSize() == -1) {
                builder.sizeUnbounded();
            } else {
                builder.sizeMax(getMaxSize());
            }

            String validationQuery = getValidationQuery();
            if (validationQuery != null) {
                builder.releaseHandler(connection -> {
                    return Flux.from(connection.createStatement(validationQuery).execute()).flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get(0)))).then();
                });
            }

            builder.evictionIdle(getMaxIdleTime());
        };
    }

    /**
     * A builder for {@link ConnectionPoolConfiguration} instances.
     * <p>
     * <i>This class is not threadsafe</i>
     */
    public static final class Builder {

        private final ConnectionFactory connectionFactory;

        private int maxSize = 10;

        private Duration maxIdleTime = Duration.ofMinutes(30);

        @Nullable
        private String validationQuery;

        private Builder(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
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
         * @throws IllegalArgumentException if {@code validationQuery} is {@code null}
         */
        public Builder maxIdleTime(Duration maxIdleTime) {
            Assert.requireNonNull(maxIdleTime, "MaxIdleTime must not be null");
            if (maxIdleTime.isNegative()) {
                throw new IllegalArgumentException("MaxIdleTime must not be negative");
            }
            this.maxIdleTime = maxIdleTime;
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
         * Returns a configured {@link ConnectionPoolConfiguration}.
         *
         * @return a configured {@link ConnectionPoolConfiguration}
         */
        public ConnectionPoolConfiguration build() {
            return new ConnectionPoolConfiguration(this.connectionFactory, this.maxIdleTime, this.maxSize, this.validationQuery);
        }

        @Override
        public String toString() {
            return "Builder{" +
                "connectionFactory='" + this.connectionFactory + '\'' +
                ", maxIdleTime='" + this.maxIdleTime + '\'' +
                ", maxSize='" + this.maxSize + '\'' +
                ", validationQuery='" + this.validationQuery + '\'' +
                '}';
        }
    }
}
