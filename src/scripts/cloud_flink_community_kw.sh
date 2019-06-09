#!/bin/bash

flink run -p 8 $1/src/scipi/jars/scipi_community.jar \
--cassandra_point ec2-18-195-41-253.eu-central-1.compute.amazonaws.com \
--keywords "$2" \
--results_path "$3" \
--total_sample_results 200 \
--sample_results_only 1 \
--community_iterations 10 \
--community_delta 0.5 \
--n_top_communities 3 \
--n_dense_community 30
