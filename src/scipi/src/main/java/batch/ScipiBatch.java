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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import publication.OagPublication;

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
        DataSet<Edge<String, PubEdgeValue>> publicationEdges = publications
                .flatMap(new PubEdgeMapper())
                .distinct(); //

        // creates the undirected publication graph
        Graph<String, NullValue, PubEdgeValue> publicationGraph =
                Graph.fromDataSet(publicationEdges, environment).getUndirected();

        
        // execute batch processing
        environment.execute("scipi batch processing");
    }

    /***************************************************
     USER DEFINED FUNCTIONS
     **************************************************/

    private static final class PubEdgeMapper implements FlatMapFunction<OagPublication, Edge<String, PubEdgeValue>> {

        @Override
        public void flatMap(OagPublication publication, Collector<Edge<String, PubEdgeValue>> out) throws Exception {

            /*
             * TYPES OF EDGES
             * --------------
             * > Author -> Co-Author (COAUTHORED)
             * > Author -> Paper (WRITTEN)
             * > Paper  -> Publisher (PUBLISHED)
             * > Paper  -> Venue (PRESENTED)
             * > Paper  -> Keyword (TAGGED)
             * > Paper  -> Field of study (DOMAIN)
             * */

            // gets information about the current publication
            String title = publication.getTitle();
            String publisher = publication.getPublisher();
            String venue = publication.getVenue();
            Set<String> keywords = publication.getKeywords();
            Set<String> fos = publication.getFos();
            Set<String> authors = publication.getAuthors();

            // if publisher is not null or empty create edge (PUBLISHED)
            if (publisher != null && !publisher.isEmpty()) {

                // create edge between publisher -> paper (PUBLISHED)
                Edge<String, PubEdgeValue> publishedEdge = new Edge<String, PubEdgeValue>();
                publishedEdge.setSource(publisher);
                publishedEdge.setTarget(title);
                publishedEdge.setValue(new PubEdgeValue(PubEdgeType.PUBLISHED, 1));
                out.collect(publishedEdge);
            }

            // if venue is not null or empty create edge (PRESENTED)
            if (venue != null && !venue.isEmpty()) {

                // create edge between paper -> venue (PRESENTED)
                Edge<String, PubEdgeValue> presentedEdge = new Edge<String, PubEdgeValue>();
                presentedEdge.setSource(title);
                presentedEdge.setTarget(venue);
                presentedEdge.setValue(new PubEdgeValue(PubEdgeType.PRESENTED, 1));
                out.collect(presentedEdge);
            }

            // if keywords is not null or empty create edges (TAGGED)
            if (keywords != null && keywords.size() > 0) {
                for (String keyword: keywords){

                    // create edge between paper -> keyword (TAGGED)
                    Edge<String, PubEdgeValue> taggedEdge = new Edge<String, PubEdgeValue>();
                    taggedEdge.setSource(title);
                    taggedEdge.setTarget(keyword);
                    taggedEdge.setValue(new PubEdgeValue(PubEdgeType.TAGGED, 1));
                    out.collect(taggedEdge);
                }
            }

            // if field of study is not not null or empty create edges (DOMAIN)
            if (fos != null && fos.size() > 0) {
                for (String field: fos){

                    // create edge between paper -> field (DOMAIN)
                    Edge<String, PubEdgeValue> domainEdge = new Edge<String, PubEdgeValue>();
                    domainEdge.setSource(title);
                    domainEdge.setTarget(field);
                    domainEdge.setValue(new PubEdgeValue(PubEdgeType.DOMAIN, 1));
                    out.collect(domainEdge);
                }
            }

            // add edges between authors (COAUTHORED)
            // add edges between author and paper (PUBLISHED)
            if (authors != null && authors.size() > 0) {

                // converts author set to array
                String[] authorList = authors.toArray(new String[authors.size()]);

                // create edge between 1st author -> paper (PUBLISHED)
                Edge<String, PubEdgeValue> writtenEdge = new Edge<String, PubEdgeValue>();
                writtenEdge.setSource(authorList[0]);
                writtenEdge.setTarget(title);
                writtenEdge.setValue(new PubEdgeValue(PubEdgeType.WRITTEN, 1));
                out.collect(writtenEdge);

                // more than one author worked on this publication
                Integer totalAuthors = authorList.length;
                if (totalAuthors > 1) {
                    for (int i = 0; i < authors.size() - 1; i++) {

                        // get current author name
                        String currentAuthor = authorList[i];

                        // check if this author is not at position 0 and add published edge
                        // create edge between other authors -> paper (PUBLISHED)
                        if (i > 0) {
                            Edge<String, PubEdgeValue> writtenOtherEdge = new Edge<String, PubEdgeValue>();
                            writtenOtherEdge.setSource(currentAuthor);
                            writtenOtherEdge.setTarget(title);
                            writtenOtherEdge.setValue(new PubEdgeValue(PubEdgeType.WRITTEN, 1));
                            out.collect(writtenOtherEdge);
                        }

                        for (int j = i + 1; j < totalAuthors; j++) {

                            // gets co-author
                            String coAuthor = authorList[j];

                            // create edge [author -> co-author] (COAUTHORED)
                            Edge<String, PubEdgeValue> coAuthoredEdge = new Edge<String, PubEdgeValue>();
                            coAuthoredEdge.setSource(currentAuthor);
                            coAuthoredEdge.setTarget(coAuthor);
                            coAuthoredEdge.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));
                            out.collect(coAuthoredEdge);
                        }
                    }
                }
            }

            //
//            // add edges between authors
//            Set<String> authors = publication.getAuthors();
//            if (authors != null && authors.size() > 0) {
//
//                // create edges between co-authors
//                String[] authorList = authors.toArray(new String[authors.size()]);
//
//                // more than one author worked on this publication
//                Integer totalAuthors = authorList.length;
//                if (totalAuthors > 1) {
//                    for (int i = 0; i < authors.size() - 1; i++) {
//                        String currentAuthor = authorList[i];
//
//                        for (int j = i + 1; j < totalAuthors; j++) {
//
//                            // gets co-author
//                            String coAuthor = authorList[j];
//
//                            // author -> co-author
//                            Edge<String, PubEdgeValue> e1 = new Edge<String, PubEdgeValue>();
//                            e1.setSource(currentAuthor);
//                            e1.setTarget(coAuthor);
//                            e1.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));
//
//                            // collects edges
//                            out.collect(e1);
//                        }
//                    }
//                }
//            }
        }
    }

}
