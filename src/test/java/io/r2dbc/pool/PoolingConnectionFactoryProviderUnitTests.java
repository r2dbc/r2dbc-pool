/*
 * Copyright 2019-2020 the original author or authors.
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
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PoolingConnectionFactoryProvider}.
 *
 * @author Mark Paluch
 * @author Rodolpho S. Couto
 */
final class PoolingConnectionFactoryProviderUnitTests {

    private final PoolingConnectionFactoryProvider provider = new PoolingConnectionFactoryProvider();

    ConnectionFactory connectionFactoryMock = mock(ConnectionFactory.class);

    Connection connectionMock = mock(Connection.class);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {

        when(connectionFactoryMock.create()).thenReturn((Publisher) Mono.just(connectionMock));
        MockConnectionFactoryProvider.setMockSupplier(() -> connectionFactoryMock);
    }

    @AfterEach
    void tearDown() {
        MockConnectionFactoryProvider.resetMockSupplier();
    }

    @Test
    void doesNotSupportWithoutDriver() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .build())).isFalse();
    }

    @Test
    void supports() {
        assertThat(this.provider.supports(ConnectionFactoryOptions.builder()
            .option(DRIVER, PoolingConnectionFactoryProvider.POOLING_DRIVER)
            .build())).isTrue();
    }

    @Test
    void returnsDriverIdentifier() {
        assertThat(this.provider.getDriver()).isEqualTo(PoolingConnectionFactoryProvider.POOLING_DRIVER);
    }

    @Test
    void shouldApplyValidationDepth() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?validationDepth=remote");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("validationDepth", ValidationDepth.REMOTE);
    }

    @Test
    void shouldApplyAcquireRetry() {

        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?acquireRetry=2");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("acquireRetry", 2);
    }

    @Test
    void shouldApplyInitialSizeAndMaxSize() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?initialSize=2&maxSize=12");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("initialSize", 2)
            .hasFieldOrPropertyWithValue("maxSize", 12);
    }

    @Test
    void shouldApplyMaxLifeTime() {

        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?maxLifeTime=PT30M");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("maxLifeTime", Duration.ofMinutes(30));
    }

    @Test
    void shouldApplyMaxAcquireTime() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?maxAcquireTime=PT30M");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("maxAcquireTime", Duration.ofMinutes(30));
    }

    @Test
    void shouldApplyMaxIdleTime() {

        ConnectionFactoryOptions options = ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?maxIdleTime=PT30M");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("maxIdleTime", Duration.ofMinutes(30));
    }

    @Test
    void shouldApplyMaxCreateConnectionTime() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?maxCreateConnectionTime=PT30M");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("maxCreateConnectionTime", Duration.ofMinutes(30));
    }

    @Test
    void shouldApplyRegisterJmx() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?registerJmx=true&name=requiredHere");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("registerJmx", true);
    }

    @Test
    void shouldApplyName() {

        ConnectionFactoryOptions options =
            ConnectionFactoryOptions.parse("r2dbc:pool:mock://host?name=UnitTest");

        ConnectionPoolConfiguration configuration = PoolingConnectionFactoryProvider.buildConfiguration(options);

        assertThat(configuration)
            .hasFieldOrPropertyWithValue("name", "UnitTest");
    }
}
