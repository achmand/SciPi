#!/bin/bash

/home/delinvas/repos/flink/build-target/bin/flink \
run -c batch.ScipiBatchAssociation $1/src/scipi/jars/scipi_association.jar \
--cassandra_point 127.0.0.1 \
--keywords "$2" \
--results_path "$3" \
--total_sample_results 200 \
--sample_results_only 1 \
--cosine_k 3 \
--cosine_similarity_threshold 0.3 \
--kw_usage_threshold 1