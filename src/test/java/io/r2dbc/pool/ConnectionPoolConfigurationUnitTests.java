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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ConnectionPoolConfiguration}.
 *
 * @author Mark Paluch
 * @author Tadaya Tsuyukubo
 * @author Steffen Kreutz
 */
final class ConnectionPoolConfigurationUnitTests {

    @Test
    void configuration() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .validationQuery("foo")
            .maxIdleTime(Duration.ofMillis(1000))
            .backgroundEvictionInterval(Duration.ofMillis(2000))
            .maxCreateConnectionTime(Duration.ofMinutes(1))
            .maxAcquireTime(Duration.ofMinutes(2))
            .maxValidationTime(Duration.ofMinutes(1))
            .initialSize(2)
            .maxSize(20)
            .name("bar")
            .registerJmx(true)
            .warmupParallelism(99)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionFactory", connectionFactoryMock)
            .hasFieldOrPropertyWithValue("validationQuery", "foo")
            .hasFieldOrPropertyWithValue("maxIdleTime", Duration.ofMillis(1000))
            .hasFieldOrPropertyWithValue("backgroundEvictionInterval", Duration.ofMillis(2000))
            .hasFieldOrPropertyWithValue("maxCreateConnectionTime", Duration.ofMinutes(1))
            .hasFieldOrPropertyWithValue("maxAcquireTime", Duration.ofMinutes(2))
            .hasFieldOrPropertyWithValue("maxValidationTime", Duration.ofMinutes(1))
            .hasFieldOrPropertyWithValue("initialSize", 2)
            .hasFieldOrPropertyWithValue("maxSize", 20)
            .hasFieldOrPropertyWithValue("name", "bar")
            .hasFieldOrPropertyWithValue("registerJmx", true)
            .hasFieldOrPropertyWithValue("warmupParallelism", 99);
    }

    @Test
    void configurationDefaults() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionFactory", connectionFactoryMock)
            .hasFieldOrPropertyWithValue("name", null)
            .hasFieldOrPropertyWithValue("validationQuery", null)
            .hasFieldOrPropertyWithValue("validationDepth", ValidationDepth.LOCAL)
            .hasFieldOrPropertyWithValue("maxIdleTime", Duration.ofMinutes(30))
            .hasFieldOrPropertyWithValue("backgroundEvictionInterval", Duration.ofMillis(-1))
            .hasFieldOrPropertyWithValue("maxCreateConnectionTime", Duration.ofMillis(-1))
            .hasFieldOrPropertyWithValue("maxAcquireTime", Duration.ofMillis(-1))
            .hasFieldOrPropertyWithValue("maxValidationTime", Duration.ofMillis(-1))
            .hasFieldOrPropertyWithValue("initialSize", 10)
            .hasFieldOrPropertyWithValue("maxSize", 10)
            .hasFieldOrPropertyWithValue("registerJmx", false)
            .hasFieldOrPropertyWithValue("warmupParallelism", 1);
    }

    @Test
    void constructorNoConnectionFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionPoolConfiguration.builder(null))
            .withMessage("ConnectionFactory must not be null");
    }

    @Test
    void invalidConfiguration() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);

        assertThatIllegalArgumentException().isThrownBy(() ->
            ConnectionPoolConfiguration.builder(connectionFactoryMock).registerJmx(true).build()
        ).withMessage("name must not be null when registering to JMX");
    }

    @Test
    void initialSizeUsesDefaultSpecified() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .maxSize(20)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("initialSize", 10)
            .hasFieldOrPropertyWithValue("maxSize", 20);
    }

    @Test
    void initialSizeUpdatedToMatchMaxSize() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .maxSize(5)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("initialSize", 5)
            .hasFieldOrPropertyWithValue("maxSize", 5);
    }

    @Test
    void maxSizeEqualInitialSizeWhenNotSpecified() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .initialSize(20)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("initialSize", 20)
            .hasFieldOrPropertyWithValue("maxSize", 20);
    }

    @Test
    void maxSizeGreaterOrEqualInitialSize() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);

        assertThatIllegalArgumentException().isThrownBy(() ->
            ConnectionPoolConfiguration.builder(connectionFactoryMock).initialSize(2).maxSize(1).build()
        ).withMessage("maxSize must be greater than or equal to initialSize");
    }
}
