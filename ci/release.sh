#!/usr/bin/env bash

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

r2dbc_pool_artifactory=$(pwd)/r2dbc-pool-artifactory

rm -rf $HOME/.m2/repository/io/r2dbc 2> /dev/null || :

cd r2dbc-pool
./mvnw deploy \
    -DaltDeploymentRepository=distribution::default::file://${r2dbc_pool_artifactory}
