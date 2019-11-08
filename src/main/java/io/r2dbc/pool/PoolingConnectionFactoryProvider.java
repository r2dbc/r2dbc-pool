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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.ValidationDepth;

import java.util.Locale;

import static io.r2dbc.pool.ConnectionPoolConfiguration.Builder;
import static io.r2dbc.pool.ConnectionPoolConfiguration.builder;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

/**
 * An implementation of {@link ConnectionFactory} for creating pooled connections to a delegated {@link ConnectionFactory}.
 *
 * @author Mark Paluch
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
     * InitialSize {@link Option}.
     */
    public static final Option<Integer> INITIAL_SIZE = Option.valueOf("initialSize");

    /**
     * MaxSize {@link Option}.
     */
    public static final Option<Integer> MAX_SIZE = Option.valueOf("maxSize");

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

        String protocol = connectionFactoryOptions.getRequiredValue(ConnectionFactoryOptions.PROTOCOL);
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
            throw new IllegalArgumentException(String.format("Could not find delegating driver %s", driverDelegate));
        }

        Builder builder = builder(connectionFactory);

        if (connectionFactoryOptions.hasOption(INITIAL_SIZE)) {
            builder.initialSize(parseIntOption(connectionFactoryOptions, INITIAL_SIZE));
        }

        if (connectionFactoryOptions.hasOption(MAX_SIZE)) {
            builder.maxSize(parseIntOption(connectionFactoryOptions, MAX_SIZE));
        }

        if (connectionFactoryOptions.hasOption(ACQUIRE_RETRY)) {
            builder.acquireRetry(parseIntOption(connectionFactoryOptions, ACQUIRE_RETRY));
        }

        if (connectionFactoryOptions.hasOption(VALIDATION_QUERY)) {

            String validationQuery = connectionFactoryOptions.getRequiredValue(VALIDATION_QUERY);
            builder.validationQuery(validationQuery);
        }

        if (connectionFactoryOptions.hasOption(VALIDATION_DEPTH)) {

            Object validationDepth = connectionFactoryOptions.getRequiredValue(VALIDATION_DEPTH);

            if (validationDepth instanceof String) {
                validationDepth = ValidationDepth.valueOf(((String) validationDepth).toUpperCase(Locale.ENGLISH));
            }

            builder.validationDepth((ValidationDepth) validationDepth);
        }

        return builder.build();
    }

    private static int parseIntOption(ConnectionFactoryOptions options, Option<?> option) {

        Object value = options.getRequiredValue(option);
        if (value instanceof Number) {
            return ((Integer) value);
        }

        if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }

        throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value));
    }

    @Override
    public boolean supports(ConnectionFactoryOptions connectionFactoryOptions) {
        Assert.requireNonNull(connectionFactoryOptions, "connectionFactoryOptions must not be null");

        String driver = connectionFactoryOptions.getValue(DRIVER);
        if (driver == null || !driver.equals(POOLING_DRIVER)) {
            return false;
        }

        return true;
    }

    @Override
    public String getDriver() {
        return POOLING_DRIVER;
    }
}
