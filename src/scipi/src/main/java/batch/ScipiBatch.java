package batch;

/*

Handles/processes data in batch found in CassandraDB using Apache Flink.

-------------
PROCESS FLOW
-------------
- 0.0: get publications from CassandraDB
- 1.0: create a graph of the input dataset

*/

// importing packages

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import publication.OagPublication;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

        // 0.0: set CassandraDB input format for OagPublication
        CassandraPojoInputFormat cassandraPojoInputFormat = new CassandraPojoInputFormat<OagPublication>(
                "SELECT * FROM scipi.oagpub;",
                new ClusterBuilder() {
                    @Override
                    protected Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint("127.0.0.1").build();
                    }
                },
                OagPublication.class
        );

        // gets the publications from CassandraDB using the specified InputFormat and TypeInformation for OagPublication POJO
        TypeInformation<OagPublication> typeInformation = TypeInformation.of(new TypeHint<OagPublication>() {
        });

        DataSet<OagPublication> publications = environment.createInput(cassandraPojoInputFormat, typeInformation);

        // 1.0: create a graph from the publications gathered from the database

        // co-authors edges
        DataSet<Edge<String, PubEdgeValue>> authorsEdges = publications
                .flatMap(new PubEdgeMapper());

        

        // execute batch processing
        environment.execute("scipi batch processing");
    }

    /***************************************************
     USER DEFINED FUNCTIONS
     **************************************************/

    private static final class PubEdgeMapper implements FlatMapFunction<OagPublication, Edge<String, PubEdgeValue>> {

        @Override
        public void flatMap(OagPublication publication, Collector<Edge<String, PubEdgeValue>> out) throws Exception {

            // add edges between authors
            Set<String> authors = publication.getAuthors();
            if (authors != null && authors.size() > 0) {

                // create edges between co-authors
                String[] authorList = authors.toArray(new String[authors.size()]);

                // more than one author worked on this publication
                Integer totalAuthors = authorList.length;
                if (totalAuthors > 1) {
                    for (int i = 0; i < authors.size() - 1; i++) {
                        String currentAuthor = authorList[i];

                        for (int j = i + 1; j < totalAuthors; j++) {

                            // gets co-author
                            String coAuthor = authorList[j];

                            // must add two edges for undirected graph
                            // author -> co-author
                            Edge<String, PubEdgeValue> e1 = new Edge<String, PubEdgeValue>();
                            e1.setSource(currentAuthor);
                            e1.setTarget(coAuthor);
                            e1.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));

                            // co-author -> author
                            Edge<String, PubEdgeValue> e2 = new Edge<String, PubEdgeValue>();
                            e2.setSource(coAuthor);
                            e2.setTarget(currentAuthor);
                            e2.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));

                            // collects edges
                            out.collect(e1);
                            out.collect(e2);
                        }
                    }
                }
            }
        }
    }

}
