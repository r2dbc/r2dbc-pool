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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

import java.util.function.Supplier;

public class MockConnectionFactoryProvider implements ConnectionFactoryProvider {

    private static Supplier<ConnectionFactory> MOCK_FACTORY = () -> {
        throw new UnsupportedOperationException("Unconfigured");
    };

    static void resetMockSupplier() {
        MOCK_FACTORY = () -> {
            throw new UnsupportedOperationException("Unconfigured");
        };
    }

    static void setMockSupplier(Supplier<ConnectionFactory> supplier) {
        MOCK_FACTORY = supplier;
    }

    @Override
    public ConnectionFactory create(ConnectionFactoryOptions connectionFactoryOptions) {
        return MOCK_FACTORY.get();
    }

    @Override
    public boolean supports(ConnectionFactoryOptions opts) {
        return opts.hasOption(ConnectionFactoryOptions.DRIVER) && opts.getRequiredValue(ConnectionFactoryOptions.DRIVER).equals(getDriver());
    }

    @Override
    public String getDriver() {
        return "mock";
    }
}
