#!/bin/bash

# Push cql files into the cassandra container
docker cp setup_cassandra/create_test_table.cql cassandra:/tmp/create_test_table.cql

# Run Cassandra CQL commands to create keyspaces and tables
echo "Running CQL script to set up keyspaces and tables..."

# create the table
docker exec -i cassandra cqlsh -f tmp/create_test_table.cql