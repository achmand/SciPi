package batch;

/*

Handles/processes data in batch found in CassandraDB using Apache Flink.

-------------
PROCESS FLOW
-------------

*/

// importing packages

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import publication.OagPublication;

public class ScipiBatch {

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

        // set CassandraDB input format for OagPublication
        CassandraInputFormat cassandraPojoInputFormat = new CassandraInputFormat<Tuple2<String, String>>(
                "SELECT doi, title FROM scipi.oagpub;",
                new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                });


//        CassandraPojoInputFormat<OagPublication> cassandraPojoInputFormat = new CassandraPojoInputFormat<OagPublication>(
//                "SELECT * FROM scipi.oagpub;",
//                new ClusterBuilder() {
//                    @Override
//                    protected Cluster buildCluster(Cluster.Builder builder) {
//                        return builder.addContactPoint("127.0.0.1").build();
//                    }
//                },
//                OagPublication.class
//        );

        // read publications from CassandraDB table "scipi.oagpub"
//        TypeInformation<String> info = TypeInformation.of(Oa.class);

        TypeInformation<Tuple2<String, String>> info = TypeInformation.of(new TypeHint<Tuple2<String, String>>(){});
        DataSet<Tuple2<String, String>> publications = environment.createInput(cassandraPojoInputFormat, info);

        publications.writeAsCsv("/home/delinvas/repos/SciPi/output");

        // execute batch processing
        environment.execute("scipi batch processing");
    }
}