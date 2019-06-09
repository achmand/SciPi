#!/bin/bash

/home/delinvas/repos/flink/build-target/bin/flink \
run -c batch.ScipiBatchCommunity $1/src/scipi/jars/scipi_community.jar \
--cassandra_point 127.0.0.1 \
--keywords "$2" \
--results_path "$3" \
--total_sample_results 200 \
--sample_results_only 1 \
--community_iterations 10 \
--community_delta 0.5 \
--n_top_communities 3 \
--n_dense_community 30
