package batch;

/*

Handles/processes data in batch found in CassandraDB using Apache Flink.

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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.CommunityDetection;
import org.apache.flink.graph.utils.GraphUtils;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
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

        // set properties for cassandra
        ClusterBuilder cassandraBuilder = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("127.0.0.1")
                        .build();
            }
        };

        // 0.0:

        // X.0: create one big graph which holds all entities in publication network
        // X.0.1: create edges from publications dataset

        // create POJO input format to get OagPublication entities from CassandraDB
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

        // TypeInformation must be specified for OagPublication POJO
        TypeInformation<OagPublication> typeInformation = TypeInformation.of(new TypeHint<OagPublication>() {
        });

        // retrieve OagPublication entities as a dataset
        DataSet<OagPublication> publications = environment.createInput(cassandraPojoInputFormat, typeInformation);

        // create vertices, map OagPublication to Vertex<String, PubVertexValue>
        DataSet<Vertex<String, PubVertexValue>> networkVertices = publications
                .flatMap(new NetworkVertexMapper())
                .distinct(0); // only unique vertices by name

        // create edges, map OagPublication to Edge<String, Double>
        DataSet<Edge<String, Double>> networkEdges = publications
                .flatMap(new NetworkEdgeMapper());

        // create an undirected publication network graph
        Graph<String, PubVertexValue, Double> networkGraph = Graph
                .fromDataSet(networkVertices, networkEdges, environment)
                .getUndirected();

        DataSet<Tuple2<String, Long>> vertexIdsInitialLabels = DataSetUtils
                .zipWithUniqueId(networkGraph.getVertexIds())
                .map(new MapFunction<Tuple2<Long, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<Long, String> value) throws Exception {
                        return new Tuple2<String, Long>(value.f1, value.f0);
                    }
                });

        Graph<String, Long, Double> communityGraph = networkGraph
                .translateVertexValues(new TranslateFunction<PubVertexValue, Long>() {
                    @Override
                    public Long translate(PubVertexValue pubVertexValue, Long o) throws Exception {
                        return pubVertexValue.getVertexValue();
                    }
                }).joinWithVertices(vertexIdsInitialLabels, new VertexJoinFunction<Long, Long>() {
                    @Override
                    public Long vertexJoin(Long defaultLabel,
                                           Long uniqueLabel) throws Exception {
                        return uniqueLabel;
                    }
                }).run(new CommunityDetection<String>(100, 0.5));

        DataSet<Tuple3<String, String, Long>> testingVerticesWithLabel = networkGraph
                .joinWithVertices(communityGraph.getVerticesAsTuple2(),
                        new VertexJoinFunction<PubVertexValue, Long>() {
                            @Override
                            public PubVertexValue vertexJoin(PubVertexValue pubVertexValue,
                                                             Long label) throws Exception {
                                pubVertexValue.setVertexValue(label);
                                return pubVertexValue;
                            }
                        }).getVertices()
                .map(new MapFunction<Vertex<String, PubVertexValue>, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(
                            Vertex<String, PubVertexValue> vertex) throws Exception {
                        return new Tuple3<String, String, Long>(
                                vertex.f0,
                                vertex.f1.getVertexType().toString(),
                                vertex.f1.getVertexValue());
                    }
                });

        DataSet<Tuple2<String, Long>> testingVertices = communityGraph
                .getVertices()
                .map(new MapFunction<Vertex<String, Long>,
                        Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Vertex<String, Long> vertex) throws Exception {
                        return new Tuple2<String, Long>(
                                vertex.f0,
                                vertex.f1);
                    }
                });

        testingVertices.writeAsCsv("/home/delinvas/repos/SciPi/output");
        testingVerticesWithLabel.writeAsCsv("/home/delinvas/repos/SciPi/output2");

        // TODO -> Only testing with authors, papers and publishers
        // map OagPublication dataset to dataset of Edge<PubVertexId, Double>
//        DataSet<Edge<PubVertexId, Double>> networkEdges = publications.flatMap(new NetworkEdgeMapper());
////
//        Graph<PubVertexId, Long, Double> networkGraph = Graph
//                .fromDataSet(networkEdges, new MapFunction<PubVertexId, Long>() {
//                    @Override
//                    public Long map(PubVertexId pubVertexId) throws Exception { // initially set vertex value to 1
//                        return 1L;
//                    }
//                }, environment)
//                .getUndirected(); // gets undirected


//        // create publication network graph from provided edges
//        Graph<PubVertexId, Long, Double> networkGraph = Graph
//                .fromDataSet(networkEdges, new MapFunction<PubVertexId, Long>() {
//                    @Override
//                    public Long map(PubVertexId pubVertexId) throws Exception { // initially set vertex value to 1
//                        return 1L;
//                    }
//                }, environment)
//                .getUndirected(); // gets undirected


//        DataSet<Edge<String, Double>> networkEdges = publications.flatMap(new NetworkEdgeMapper2());
//
//        Graph<String, Long, Double> networkGraph = Graph
//                .fromDataSet(networkEdges2, new MapFunction<String, Long>() {
//                    @Override
//                    public Long map(String pubVertexId) throws Exception { // initially set vertex value to 1
//                        return 1L;
//                    }
//                }, environment)
//                .getUndirected(); // gets undirected


        // X.0: now to apply the CommunityDetection algorithm we must have a Graph
        // with Vertex values as Long type and Edges values as Double

        // we need to set the a unique Long for vertices so CommunityDetection algorithm to work
        // first we create a dataset of (PubVertexId, Long) and create a unique id
        // this unique id will be used as a label in the algorithm
//        DataSet<Tuple2<PubVertexId, Long>> vertexIdsInitialLabels = DataSetUtils
//                .zipWithUniqueId(networkGraph.getVertexIds())
//                .map(new MapFunction<Tuple2<Long, PubVertexId>, Tuple2<PubVertexId, Long>>() {
//                    @Override
//                    public Tuple2<PubVertexId, Long> map(Tuple2<Long, PubVertexId> value) throws Exception {
//                        return new Tuple2<PubVertexId, Long>(value.f1, value.f0);
//                    }
//                });

        // join with vertices and set value of each vertex to the unique generated id
        // which will be used as a label in the algorithm
//        Graph<PubVertexId, Long, Double> networkGraphWithLabels = networkGraph
//                .joinWithVertices(vertexIdsInitialLabels, new VertexJoinFunction<Long, Long>() {
//                    @Override
//                    public Long vertexJoin(Long defaultLabel,
//                                           Long uniqueLabel) throws Exception {
//                        return uniqueLabel;
//                    }
//                }).run(new CommunityDetection<PubVertexId>(100, 0.5));

        // now that each vertex value is set to a label, any two vertices with the same label
        // belong the the same community
//        DataSet<Tuple3<String, String, Double>> communityEdges = networkGraphWithLabels
//                .getEdges()
//                .map(new MapFunction<Edge<PubVertexId, Double>, Tuple3<String, String, Double>>() {
//                    @Override
//                    public Tuple3<String, String, Double> map(
//                            Edge<PubVertexId, Double> edge) throws Exception {
//                        return new Tuple3<String, String, Double>(
//                                edge.f0.getVertexName(),
//                                edge.f1.getVertexName(),
//                                edge.f2);
//                    }
//                }).distinct(); // only get unique edges (since we are using undirected graph)

//        DataSet<Tuple3<String, String, Long>> communityVertices = networkGraph
//                .getVertices()
//                .map(new MapFunction<Vertex<String, Long>,
//                        Tuple3<String, String, Long>>() {
//                    @Override
//                    public Tuple3<String, String, Long> map(Vertex<String, Long> vertex) throws Exception {
//                        return new Tuple3<String, String, Long>(
//                                vertex.f0,
//                                "",
//                                vertex.f1);
//                    }
//                });

//        DataSet<Tuple3<String, String, Long>> communityVertices = networkGraph
//                .getVertices()
//                .map(new MapFunction<Vertex<PubVertexId, Long>,
//                        Tuple3<String, String, Long>>() {
//                    @Override
//                    public Tuple3<String, String, Long> map(Vertex<PubVertexId, Long> vertex) throws Exception {
//                        return new Tuple3<String, String, Long>(
//                                vertex.f0.getVertexName(),
//                                "",
//                                vertex.f1);
//                    }
//                });

//        communityEdges.writeAsCsv("/home/delinvas/repos/SciPi/output");
//        communityVertices.writeAsCsv("/home/delinvas/repos/SciPi/output2");
        environment.execute("scipi batch processing");
    }

    private static Edge<String, Double> CreateEdge(String source, String target, Double value) {
        Edge<String, Double> edge = new Edge<String, Double>();
        edge.setSource(source);
        edge.setTarget(target);
        edge.setValue(value);
        return edge;
    }

    /***************************************************
     USER DEFINED FUNCTIONS
     **************************************************/

    private static final class NetworkVertexMapper implements FlatMapFunction<OagPublication, Vertex<String, PubVertexValue>> {

        @Override
        public void flatMap(OagPublication publication,
                            Collector<Vertex<String, PubVertexValue>> out) throws Exception {

            // TODO -> temporary
            Set<String> fos = publication.getFos();

            if (fos == null) {
                return;
            }

            if (!fos.contains("computer science")) {
                return;
            }

            // create vertex (PAPER)
            String title = publication.getTitle();
            out.collect(new Vertex<String, PubVertexValue>(
                    title,
                    new PubVertexValue(1L, PubVertexType.PAPER)));

            // if publisher is not null or empty, create vertex (PUBLISHER)
            String publisher = publication.getPublisher();
            if (publisher != null && !publisher.isEmpty()) {
                out.collect(new Vertex<String, PubVertexValue>(
                        publisher,
                        new PubVertexValue(1L, PubVertexType.PUBLISHER)));
            }

            // if venue is not null or empty, create vertex (VENUE)
            String venue = publication.getVenue();
            if (venue != null && !venue.isEmpty()) {
                out.collect(new Vertex<String, PubVertexValue>(
                        venue,
                        new PubVertexValue(1L, PubVertexType.VENUE)));
            }

            // add vertices for authors (AUTHOR)
            Set<String> authors = publication.getAuthors();
            for (String author : authors) {
                out.collect(new Vertex<String, PubVertexValue>(
                        author,
                        new PubVertexValue(1L, PubVertexType.AUTHOR)));
            }
        }
    }

    private static final class NetworkEdgeMapper implements FlatMapFunction<OagPublication, Edge<String, Double>> {

        @Override
        public void flatMap(OagPublication publication,
                            Collector<Edge<String, Double>> out) throws Exception {

            // gets information about the current publication
            Set<String> keywords = publication.getKeywords();
            Set<String> fos = publication.getFos();

            // TODO -> For testing purposes at this stage every edge must be created
            if (fos == null) {
                return;
            }

            if (!fos.contains("computer science")) {
                return;
            }

            // gets paper title
            String paper = publication.getTitle();

            // if publisher is not null or empty create edge, PAPER -> PUBLISHER
            String publisher = publication.getPublisher();
            if (publisher != null && !publisher.isEmpty()) {
                out.collect(CreateEdge(paper, publisher, 1.0));
            }

            // if venue is not null or empty create edge, PAPER -> VENUE
            String venue = publication.getVenue();
            if (venue != null && !venue.isEmpty()) {
                out.collect(CreateEdge(paper, venue, 1.0));
            }

            // add edges between authors, AUTHOR -> COAUTHOR
            // add edges between author and publication, AUTHOR -> PAPER
            Set<String> authors = publication.getAuthors();

            // converts author set to array
            String[] authorList = authors.toArray(new String[authors.size()]);

            // create edge between main author and paper, AUTHOR -> PAPER
            out.collect(CreateEdge(authorList[0], paper, 1.0));

            // add edges between coauthors if there are any
            Integer totalAuthors = authorList.length;
            if (totalAuthors > 1) {

                // loop in authors
                for (int i = 0; i < totalAuthors - 1; i++) {

                    // get current author name
                    String currentAuthor = authorList[i];

                    // if not main author add edge between coauthor and paper, AUTHOR -> PAPER
                    if (i > 0) {
                        out.collect(CreateEdge(currentAuthor, paper, 1.0));
                    }

                    // add edges between coauthors, AUTHOR -> COAUTHOR
                    for (int j = i + 1; j < totalAuthors; j++) {
                        out.collect(CreateEdge(currentAuthor, authorList[j], 1.0));
                    }
                }
            }
        }
    }

}
