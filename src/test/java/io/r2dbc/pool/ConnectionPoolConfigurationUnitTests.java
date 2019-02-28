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

import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ConnectionPoolConfiguration}.
 *
 * @author Mark Paluch
 */
final class ConnectionPoolConfigurationUnitTests {

    @Test
    void configuration() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock)
            .validationQuery("foo")
            .maxIdleTime(Duration.ofMillis(1000))
            .maxSize(20)
            .build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionFactory", connectionFactoryMock)
            .hasFieldOrPropertyWithValue("validationQuery", "foo")
            .hasFieldOrPropertyWithValue("maxIdleTime", Duration.ofMillis(1000))
            .hasFieldOrPropertyWithValue("maxSize", 20);
    }

    @Test
    void configurationDefaults() {
        ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactoryMock).build();

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("connectionFactory", connectionFactoryMock)
            .hasFieldOrPropertyWithValue("validationQuery", null)
            .hasFieldOrPropertyWithValue("maxIdleTime", Duration.ofMinutes(30))
            .hasFieldOrPropertyWithValue("maxSize", 10);
    }

    @Test
    void constructorNoConnectionFactory() {
        assertThatIllegalArgumentException().isThrownBy(() -> ConnectionPoolConfiguration.builder(null))
            .withMessage("ConnectionFactory must not be null");
    }
}
