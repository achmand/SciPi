#!/bin/bash

/home/delinvas/repos/flink/build-target/bin/flink \
run -c batch.ScipiBatchTopics $1/src/scipi/jars/scipi_topics.jar \
--cassandra_point 127.0.0.1 \
--n_occurrences $2 \
--results_path "$3" 