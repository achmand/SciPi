#!/bin/bash

flink run -p 8 \
run -c batch.ScipiBatchTopics $1/src/scipi/jars/scipi_topics.jar \
--cassandra_point ec2-18-195-41-253.eu-central-1.compute.amazonaws.com \
--n_occurrences $2 \
--results_path "$3" 