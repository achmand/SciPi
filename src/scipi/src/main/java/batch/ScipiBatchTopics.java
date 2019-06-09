package batch;


/*
Handles/processes data in batch found in CassandraDB using Apache Flink (Association/Correlation Analysis.

Parameters
----------
> cassandra_point: the cassandra point (IP) that the driver uses to discover the cluster topology (local execution use 127.0.0.1)
> results_path: the path where to save all results
> n_occurrences: the number of occurrences for the keyword to be outputted
*/

// importing packages

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class ScipiBatchTopics {

    public static void main(String[] args) throws Exception {

        // returns the stream execution environment (the context 'Local or Remote' in which a program is executed)
        // LocalEnvironment will cause execution in the current JVM
        // RemoteEnvironment will cause execution on a remote setup
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // provides utility methods for reading and parsing the program arguments
        // in this tutorial we will have to provide the input file and the output file as arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        // register parameters globally so it can be available for each node in the cluster
        environment.getConfig().setGlobalJobParameters(parameters);

        // gets cassandra points from input
        // the cassandra point (IP) that the driver uses to discover the cluster topology
        // the driver will retrieve the address of the other nodes automatically
        final String cassandraPoint = parameters.get("cassandra_point"); // for local execution use 127.0.0.1

        // set up properties for cassandra cluster
        ClusterBuilder cassandraBuilder = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint(cassandraPoint)
                        .build();
            }
        };

        // gets inputs for results save only samples, paths, etc
        final String resultsPath = parameters.get("results_path");

        // create tuple input format to get keyword entities from CassandraDB
        CassandraInputFormat cassandraInputFormatKw = new CassandraInputFormat<Tuple2<String, Long>>(
                "SELECT keyword_name, keyword_count FROM scipi.keywords;",
                cassandraBuilder
        );

        // we need to specify TypeInformation for keywords
        TypeInformation<Tuple2<String, Long>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        });

        // retrieve keyword entities as a dataset from Cassandra
        DataSet<Tuple2<String, Long>> keywords = environment.createInput(cassandraInputFormatKw, typeInformation);

        // filter keywords which have >= the occurrence specified
        final Integer nOccurrences = parameters.getInt("n_occurrences");
        keywords.filter(new FilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> value) throws Exception {
                return (value.f1 >= nOccurrences);
            }
        }).writeAsCsv(resultsPath + "/topicsKeywords.csv",
                FileSystem.WriteMode.OVERWRITE // overwrite existing file
        );

        // create tuple input format to get fos entities from CassandraDB
        CassandraInputFormat cassandraInputFormatFos = new CassandraInputFormat<Tuple2<String, Long>>(
                "SELECT field_study_name, field_study_count FROM scipi.field_study;",
                cassandraBuilder
        );

        // retrieve keyword entities as a dataset from Cassandra
        DataSet<Tuple2<String, Long>> fos = environment.createInput(cassandraInputFormatFos, typeInformation);
        fos.filter(new FilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> value) throws Exception {
                return (value.f1 >= nOccurrences);
            }
        }).writeAsCsv(resultsPath + "/topicsFos.csv",
                FileSystem.WriteMode.OVERWRITE // overwrite existing file
        );

        // execute job
        environment.execute("scipi Topics");
    }
}
