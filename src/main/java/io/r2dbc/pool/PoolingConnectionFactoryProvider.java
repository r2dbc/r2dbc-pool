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
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.function.Function;

import static io.r2dbc.pool.ConnectionPoolConfiguration.Builder;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

/**
 * An implementation of {@link ConnectionFactory} for creating pooled connections to a delegated {@link ConnectionFactory}.
 *
 * @author Mark Paluch
 * @author Rodolfo Beletatti
 * @author Rodolpho S. Couto
 * @author Todd Ginsberg
 * @author Gabriel Calin
 */
public class PoolingConnectionFactoryProvider implements ConnectionFactoryProvider {

    /*
     * Driver option value.
     */
    public static final String POOLING_DRIVER = "pool";

    /**
     * AcquireRetry {@link Option}.
     */
    public static final Option<Integer> ACQUIRE_RETRY = Option.valueOf("acquireRetry");

    /**
     * Background eviction interval {@link Option}.
     *
     * @since 0.8.7
     */
    public static final Option<Integer> BACKGROUND_EVICTION_INTERVAL = Option.valueOf("backgroundEvictionInterval");

    /**
     * InitialSize {@link Option}.
     */
    public static final Option<Integer> INITIAL_SIZE = Option.valueOf("initialSize");

    /**
     * MaxSize {@link Option}.
     */
    public static final Option<Integer> MAX_SIZE = Option.valueOf("maxSize");

    /**
     * MinIdle {@link Option}.
     *
     * @since 0.9.1
     */
    public static final Option<Integer> MIN_IDLE = Option.valueOf("minIdle");

    /**
     * MaxAcquireTime {@link Option}.
     *
     * @since 0.8.3
     */
    public static final Option<Duration> MAX_ACQUIRE_TIME = Option.valueOf("maxAcquireTime");

    /**
     * MaxCreateConnectionTime {@link Option}.
     *
     * @since 0.8.3
     */
    public static final Option<Duration> MAX_CREATE_CONNECTION_TIME = Option.valueOf("maxCreateConnectionTime");

    /**
     * MaxIdleTime {@link Option}.
     *
     * @since 0.8.3
     */
    public static final Option<Duration> MAX_IDLE_TIME = Option.valueOf("maxIdleTime");

    /**
     * MaxLifeTime {@link Option}.
     *
     * @since 0.8.3
     */
    public static final Option<Duration> MAX_LIFE_TIME = Option.valueOf("maxLifeTime");

    /**
     * MaxValidationTime {@link Option}.
     *
     * @since 0.9.2
     */
    public static final Option<Duration> MAX_VALIDATION_TIME = Option.valueOf("maxValidationTime");

    /**
     * Name of the Connection Pool {@link Option}
     *
     * @since 0.8.5
     */
    public static final Option<String> POOL_NAME = Option.valueOf("poolName");

    /**
     * {@link Option} to configure whether to register to JMX.
     *
     * @since 0.8.5
     */
    public static final Option<Boolean> REGISTER_JMX = Option.valueOf("registerJmx");

    /**
     * {@link Option} to configure a {@code Lifecycle.postAllocate} function.
     *
     * @since 0.9
     */
    public static final Option<Function<? super Connection, ? extends Publisher<Void>>> POST_ALLOCATE = Option.valueOf("postAllocate");

    /**
     * {@link Option} to configure a {@code Lifecycle.preRelease} function.
     *
     * @since 0.9
     */
    public static final Option<Function<? super Connection, ? extends Publisher<Void>>> PRE_RELEASE = Option.valueOf("preRelease");

    /**
     * ValidationQuery {@link Option}.
     */
    public static final Option<String> VALIDATION_QUERY = Option.valueOf("validationQuery");

    /**
     * ValidationDepth {@link Option}.
     */
    public static final Option<ValidationDepth> VALIDATION_DEPTH = Option.valueOf("validationDepth");

    private static final String COLON = ":";

    /**
     * Create a new pooling {@link ConnectionFactory} from given {@link ConnectionFactoryOptions}.
     *
     * @param connectionFactoryOptions a collection of {@link ConnectionFactoryOptions}
     * @return the pooling {@link ConnectionFactory}
     * @throws IllegalArgumentException if {@code connectionFactoryOptions} is {@code null}
     * @throws IllegalStateException    if there is no value for {@link ConnectionFactoryOptions#PROTOCOL}
     * @throws IllegalArgumentException if {@link ConnectionFactoryOptions#PROTOCOL} has invalid format
     * @throws IllegalArgumentException if delegating {@link ConnectionFactory} cannot be found
     */
    @Override
    public ConnectionPool create(ConnectionFactoryOptions connectionFactoryOptions) {

        return new ConnectionPool(buildConfiguration(connectionFactoryOptions));
    }

    static ConnectionPoolConfiguration buildConfiguration(ConnectionFactoryOptions connectionFactoryOptions) {

        String protocol = connectionFactoryOptions.getRequiredValue(ConnectionFactoryOptions.PROTOCOL).toString();
        if (protocol.trim().length() == 0) {
            throw new IllegalArgumentException(String.format("Protocol %s is not valid.", protocol));
        }
        String[] protocols = protocol.split(COLON, 2);
        String driverDelegate = protocols[0];

        // when protocol does NOT contain COLON, the length becomes 1
        String protocolDelegate = protocols.length == 2 ? protocols[1] : "";

        ConnectionFactoryOptions newOptions = ConnectionFactoryOptions.builder()
            .from(connectionFactoryOptions)
            .option(ConnectionFactoryOptions.DRIVER, driverDelegate)
            .option(ConnectionFactoryOptions.PROTOCOL, protocolDelegate)
            .build();

        // Run discovery again to find the actual connection factory
        ConnectionFactory connectionFactory = ConnectionFactories.find(newOptions);
        if (connectionFactory == null) {
            throw new IllegalArgumentException(String.format("Could not find delegating driver [%s]", driverDelegate));
        }

        Builder builder = ConnectionPoolConfiguration.builder(connectionFactory);
        OptionMapper mapper = OptionMapper.create(newOptions);

        mapper.from(BACKGROUND_EVICTION_INTERVAL).as(OptionMapper::toDuration).to(builder::backgroundEvictionInterval);
        mapper.from(INITIAL_SIZE).as(OptionMapper::toInteger).to(builder::initialSize);
        mapper.from(MAX_SIZE).as(OptionMapper::toInteger).to(builder::maxSize);
        mapper.from(MIN_IDLE).as(OptionMapper::toInteger).to(builder::minIdle);
        mapper.from(ACQUIRE_RETRY).as(OptionMapper::toInteger).to(builder::acquireRetry);
        mapper.from(MAX_ACQUIRE_TIME).as(OptionMapper::toDuration).to(builder::maxAcquireTime);
        mapper.from(MAX_CREATE_CONNECTION_TIME).as(OptionMapper::toDuration).to(builder::maxCreateConnectionTime);
        mapper.from(MAX_LIFE_TIME).as(OptionMapper::toDuration).to(builder::maxLifeTime);
        mapper.from(MAX_IDLE_TIME).as(OptionMapper::toDuration).to(builder::maxIdleTime);
        mapper.from(MAX_VALIDATION_TIME).as(OptionMapper::toDuration).to(builder::maxValidationTime);
        mapper.fromExact(POOL_NAME).to(builder::name);
        mapper.fromExact(POST_ALLOCATE).to(builder::postAllocate);
        mapper.fromExact(PRE_RELEASE).to(builder::preRelease);
        mapper.from(REGISTER_JMX).as(OptionMapper::toBoolean).to(builder::registerJmx);
        mapper.fromExact(VALIDATION_QUERY).to(builder::validationQuery);
        mapper.from(VALIDATION_DEPTH).as(validationDepth -> OptionMapper.toEnum(validationDepth, ValidationDepth.class)).to(builder::validationDepth);

        return builder.build();
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        Object driver = connectionFactoryOptions.getValue(DRIVER);

        return driver != null && driver.equals(POOLING_DRIVER);
    }

    @Override
    public String getDriver() {
        return POOLING_DRIVER;
    }

}
