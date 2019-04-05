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
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConnectionFactory} discovery.
 *
 * @author Mark Paluch
 */
final class DiscoveryUnitTests {

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
    void shouldCreateConnectionPool() {

        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "pool")
            .option(ConnectionFactoryOptions.PROTOCOL, "mock").build();

        ConnectionFactory connectionFactory = ConnectionFactories.get(options);

        assertThat(connectionFactory).isInstanceOf(ConnectionPool.class).hasFieldOrPropertyWithValue("factory", connectionFactoryMock);
    }
}
