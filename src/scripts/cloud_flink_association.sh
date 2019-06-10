#!/bin/bash

flink run -p 8 $1/src/scipi/jars/scipi_association.jar \
--cassandra_point ec2-18-195-41-253.eu-central-1.compute.amazonaws.com \
--keywords "$2" \
--results_path "$3" \
--total_sample_results 200 \
--sample_results_only 0 \
--cosine_k 3 \
--cosine_similarity_threshold 0.3 \
--kw_usage_threshold 1