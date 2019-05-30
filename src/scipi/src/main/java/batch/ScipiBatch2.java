//package batch;
//
///*
//
//Handles/processes data in batch found in CassandraDB using Apache Flink.
//
//-------------
//PROCESS FLOW
//-------------
//- 0.0: get publications from CassandraDB
//- 1.0: create a graph of the input dataset
//
//*/
//
//// importing packages
//
//import com.datastax.driver.core.Cluster;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.DataSetUtils;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
//import org.apache.flink.graph.Edge;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.graph.Vertex;
//import org.apache.flink.graph.VertexJoinFunction;
//import org.apache.flink.graph.library.CommunityDetection;
//import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
//import org.apache.flink.util.Collector;
//import publication.OagPublication;
//
//import java.util.HashSet;
//import java.util.Set;
//
//public class ScipiBatch2 {
//
//    public static void main(String[] args) throws Exception {
//
//        // returns the stream execution environment (the context 'Local or Remote' in which a program is executed)
//        // LocalEnvironment will cause execution in the current JVM
//        // RemoteEnvironment will cause execution on a remote setup
//        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
//
//        // provides utility methods for reading and parsing the program arguments
//        // in this tutorial we will have to provide the input file and the output file as arguments
//        final ParameterTool parameters = ParameterTool.fromArgs(args);
//
//        // register parameters globally so it can be available for each node in the cluster
//        environment.getConfig().setGlobalJobParameters(parameters);
//
//        // set properties for cassandra
//        ClusterBuilder cassandraBuilder = new ClusterBuilder() {
//            @Override
//            public Cluster buildCluster(Cluster.Builder builder) {
//                return builder.addContactPoint("127.0.0.1")
//                        .build();
//            }
//        };
//
////        // truncate previous top 100 keywords before processing/getting top 100
////        Session session = cassandraBuilder.getCluster().connect("scipi");
////        session.execute("TRUNCATE topkw");
////        session.close();
//
////        // 0.0: batch processing keyword and field of study (sort and save top 100)
////        CassandraInputFormat keywordsInputFormat = new CassandraInputFormat<Tuple2<String, Integer>>(
////                "SELECT keyword, count FROM scipi.oagkw;",
////                cassandraBuilder
////        );
////
////        // type information for keywords
//        //
////            // add edges between authors
////            Set<String> authors = publication.getAuthors();
////            if (authors != null && authors.size() > 0) {
////
////                // create edges between co-authors
////                String[] authorList = authors.toArray(new String[authors.size()]);
////
////                // more than one author worked on this publication
////                Integer totalAuthors = authorList.length;
////                if (totalAuthors > 1) {
////                    for (int i = 0; i < authors.size() - 1; i++) {
////                        String currentAuthor = authorList[i];
////
////                        for (int j = i + 1; j < totalAuthors; j++) {
////
////                            // gets co-author
////                            String coAuthor = authorList[j];
////
////                            // author -> co-author
////                            Edge<String, PubEdgeValue> e1 = new Edge<String, PubEdgeValue>();
////                            e1.setSource(currentAuthor);
////                            e1.setTarget(coAuthor);
////                            e1.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));
////
////                            // collects edges
////                            out.collect(e1);
////                        }
////                    }
////                }
////            }
////        TypeInformation<Tuple2<String, Integer>> keywordTypeInformation = TypeInformation.of(
////                new TypeHint<Tuple2<String, Integer>>() {
////                });
////
////        DataSet<Tuple2<String, Integer>> sortedKeywords = environment
////                .createInput(keywordsInputFormat, keywordTypeInformation)
////                .sortPartition(1, Order.DESCENDING)
////                .setParallelism(1)
////                .first(100);
////
////        sortedKeywords.output(new CassandraTupleOutputFormat<Tuple2<String, Integer>>(
////                "INSERT INTO scipi.topkw(keyword, count) values (?, ?);",
////                cassandraBuilder));
//
//        // 0.0: set CassandraDB input format for OagPublication
//        CassandraPojoInputFormat cassandraPojoInputFormat = new CassandraPojoInputFormat<OagPublication>(
//                "SELECT * FROM scipi.oagpub;",
//                new ClusterBuilder() {
//                    @Override
//                    protected Cluster buildCluster(Cluster.Builder builder) {
//                        return builder.addContactPoint("127.0.0.1").build();
//                    }
//                },
//                OagPublication.class
//        );
//
//        // gets the publications from CassandraDB using the specified InputFormat and TypeInformation for OagPublication POJO
//        TypeInformation<OagPublication> typeInformation = TypeInformation.of(new TypeHint<OagPublication>() {
//        });
//
//        DataSet<OagPublication> publications = environment.createInput(cassandraPojoInputFormat, typeInformation);
//
//        // 1.0: create a graph from the publications gathered from the database
//
//
//        // map publication entries to vertices
//        DataSet<Vertex<String, Long>> publicationVertices = publications
//                .flatMap(new PubVertexMapper2())
//                .distinct(); // get only distinct vertices
//
//        // map publication entries to edges
//        DataSet<Edge<String, Double>> publicationEdges = publications
//                .flatMap(new PubEdgeMapper2())
//                .distinct(); // get only distinct edges
//
//
//        // create publication graph
//        Graph<String, Long, Double> publicationGraph = Graph
//                .fromDataSet(publicationVertices, publicationEdges, environment)
//                .getUndirected();
//
//        DataSet<Tuple2<String, Long>> idsWithInitialLabels = DataSetUtils
//                .zipWithUniqueId(publicationGraph.getVertexIds())
//                .map(new MapFunction<Tuple2<Long, String>, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(Tuple2<Long, String> value) throws Exception {
//                        return new Tuple2<String, Long>(value.f1, value.f0);
//                    }
//                });
//
//        Graph<String, Long, Double> communityGraphWithLabels = publicationGraph
//                .joinWithVertices(idsWithInitialLabels, new VertexJoinFunction<Long, Long>() {
//                    @Override
//                    public Long vertexJoin(Long aLong, Long aLong2) throws Exception {
//                        return aLong2;
//                    }
//                });
//
//        Graph<String, Long, Double> communityGraph = communityGraphWithLabels
//                .run(new CommunityDetection<String>(100, 0.8));
//
//        // get top three labels from community graph
//        final HashSet<Long> topCommunities = new HashSet<Long>(communityGraph
//                .getVertices()
//                .map(new MapFunction<Vertex<String, Long>, Tuple2<Long, Long>>() {
//                    @Override
//                    public Tuple2<Long, Long> map(Vertex<String, Long> vertex) throws Exception {
//                        return new Tuple2<Long, Long>(vertex.f1, 1L);
//                    }
//                }).groupBy(0)
//                .sum(1)
//                .sortPartition(1, Order.DESCENDING)
//                .setParallelism(1)
//                .first(3)
//                .map(new MapFunction<Tuple2<Long, Long>, Long>() {
//                    @Override
//                    public Long map(Tuple2<Long, Long> longLongTuple2) throws Exception {
//                        return longLongTuple2.f0;
//                    }
//                }).collect());
//
//        DataSet<Vertex<String, Long>> filteredVertices = communityGraph
//                .getVertices()
//                .filter(new FilterFunction<Vertex<String, Long>>() {
//                    @Override
//                    public boolean filter(Vertex<String, Long> vertex) throws Exception {
//                        return topCommunities.contains(vertex.f1);
//                    }
//                });
//
//
//        filteredVertices.writeAsCsv("/home/delinvas/repos/SciPi/output");
//
//        DataSet<Edge<String, Double>> communityEdges = communityGraph
//                .filterOnVertices(new FilterFunction<Vertex<String, Long>>() {
//                    @Override
//                    public boolean filter(Vertex<String, Long> vertex) throws Exception {
//                        return topCommunities.contains(vertex.f1);
//                    }
//                }).getEdges().distinct();
//
//
//        communityEdges.writeAsCsv("/home/delinvas/repos/SciPi/output2");
//
////
////                        Graph < String, Long, Double > communityGraph = publicationGraph
////                                .run(new CommunityDetection<String>(100, 0.8));
////
////        communityGraph
////                .getVertices()
////                .writeAsCsv("/home/delinvas/repos/SciPi/output");
//
//
//        // create publication graph
////        Graph<String, PubVertexValue, PubEdgeValue> publicationGraph = Graph
////                .fromDataSet(publicationVertices, publicationEdges, environment)
////                .getUndirected();
//
//        // detect dense communities of interest using label propagation
////        DataSet<Tuple2<String, Long>> idsWithInitialLabels = DataSetUtils
////                .zipWithUniqueId(publicationGraph.getVertexIds())
////                .map(new MapFunction<Tuple2<Long, String>, Tuple2<String, Long>>() {
////                    @Override
////                    public Tuple2<String, Long> map(Tuple2<Long, String> value) throws Exception {
////                        return new Tuple2<String, Long>(value.f1, value.f0);
////                    }
////                });
//
//        //publicationGraph.run(new CommunityDetection<String>(10, 0.5));
////        DataSet<Vertex<String, PubVertexValue>> verticesWithCommunity = publicationGraph
////                .joinWithVertices(idsWithInitialLabels, new VertexJoinFunction<PubVertexValue, Long>() {
////                    @Override
////                    public PubVertexValue vertexJoin(PubVertexValue pubVertexValue,
////                                                     Long label) throws Exception {
////                        return new PubVertexValue(label, pubVertexValue.getVertexType());
////                    }
////                }).run(new CommunityDetection<String>(10, 0.5));
//
////        DataSet<Tuple3<String, String, Long>> pubCommunities = verticesWithCommunity
////                .map(new MapFunction<Vertex<String, PubVertexValue>, Tuple3<String, String, Long>>() {
////                    @Override
////                    public Tuple3<String, String, Long> map(Vertex<String, PubVertexValue> value) throws Exception {
////                        return new Tuple3<String, String, Long>(value.f0, value.f1.getVertexType().toString(), value.f1.getLabel());
////                    }
////                });
//
////        // finding the top 3 labels to get the top 3 dense communities
////        DataSet<Tuple2<Long, Long>> labelCommunityCount = verticesWithCommunity
////                .map(new MapFunction<Vertex<String, PubVertexValue>, Tuple2<Long, Long>>() {
////                    @Override
////                    public Tuple2<Long, Long> map(Vertex<String, PubVertexValue> value) throws Exception {
////                        return new Tuple2<Long, Long>(value.f1.getLabel(), 1L);
////                    }
////                }).groupBy(0)
////                .sum(1);
//
////        final List<Long> topCommunities = labelCommunityCount
////                .sortPartition(1, Order.DESCENDING)
////                .setParallelism(1)
////                .first(3)
////                .map(new MapFunction<Tuple2<Long, Long>, Long>() {
////                    @Override
////                    public Long map(Tuple2<Long, Long> value) throws Exception {
////                        return value.f0;
////                    }
////                }).collect();
////
////        // filter only top 3 dense communities
////        DataSet<Tuple3<String, String, Long>>  denseCommunities = pubCommunities
////                .filter(new FilterFunction<Tuple3<String, String, Long>>() {
////            @Override
////            public boolean filter(Tuple3<String, String, Long> value) throws Exception {
////                return topCommunities.contains(value.f2);
////            }
////        });
//
////        pubCommunities.writeAsCsv("/home/delinvas/repos/SciPi/output1");
////        labelCommunityCount.writeAsCsv("/home/delinvas/repos/SciPi/output2");
////        denseCommunities.writeAsCsv("/home/delinvas/repos/SciPi/output3");
//
//        // creates the undirected publication graph
////        Graph<String, Long, PubEdgeValue> publicationGraph =
////                Graph.fromDataSet(publicationEdges, new MapFunction<String, Long>() {
////                    @Override
////                    public Long map(String s) throws Exception {
////                        return 1L;
////                    }
////                }, environment).getUndirected();
//
//
//        // detect dense communities of interest using label propagation
//        // initialize each vertex with a unique numeric label
////        DataSet<Tuple2<String, Long>> idsWithInitialLabels = DataSetUtils
////                .zipWithUniqueId(publicationGraph.getVertexIds())
////                .map(new MapFunction<Tuple2<Long, String>, Tuple2<String, Long>>() {
////                    @Override
////                    public Tuple2<String, Long> map(Tuple2<Long, String> value) throws Exception {
////                        return new Tuple2<String, Long>(value.f1, value.f0);
////                    }
////                });
////
////        DataSet<Vertex<String, Long>> verticesWithCommunity = publicationGraph
////                .joinWithVertices(idsWithInitialLabels, new VertexJoinFunction<Long, Long>() {
////                    @Override
////                    public Long vertexJoin(Long aLong, Long aLong2) throws Exception {
////                        return aLong2;
////                    }
////                }).run(new LabelPropagation<String, Long, PubEdgeValue>(100));
//
////        verticesWithCommunity.writeAsCsv("/home/delinvas/repos/SciPi/output");
//
//        // execute batch processing
//        environment.execute("scipi batch processing");
//    }
//
//    /***************************************************
//     USER DEFINED FUNCTIONS
//     **************************************************/
//
//    private static final class PubEdgeMapper2 implements FlatMapFunction<OagPublication, Edge<String, Double>> {
//
//        @Override
//        public void flatMap(OagPublication publication, Collector<Edge<String, Double>> out) throws Exception {
//
//            /*
//             * TYPES OF EDGES
//             * --------------
//             * > Author -> Co-Author (COAUTHORED)
//             * > Author -> Paper (WRITTEN)
//             * > Paper  -> Publisher (PUBLISHED)
//             * > Paper  -> Venue (PRESENTED)
//             * > Paper  -> Keyword (TAGGED)
//             * > Paper  -> Field of study (DOMAIN)
//             * */
//
//            // gets information about the current publication
//            String title = publication.getTitle();
//            String publisher = publication.getPublisher();
//            String venue = publication.getVenue();
//            Set<String> keywords = publication.getKeywords();
//            Set<String> fos = publication.getFos();
//            Set<String> authors = publication.getAuthors();
//
//            if (fos == null) {
//                return;
//            }
//
//            if (!fos.contains("computer science")) {
//                return;
//            }
//
//            // if publisher is not null or empty create edge (PUBLISHED)
//            if (publisher != null && !publisher.isEmpty()) {
//
//                // create edge between publisher -> paper (PUBLISHED)
//                Edge<String, Double> publishedEdge = new Edge<String, Double>();
//                publishedEdge.setSource(publisher);
//                publishedEdge.setTarget(title);
//                publishedEdge.setValue(1.0);
//                out.collect(publishedEdge);
//            }
//
//            // if venue is not null or empty create edge (PRESENTED)
//            if (venue != null && !venue.isEmpty()) {
//
//                // create edge between paper -> venue (PRESENTED)
//                Edge<String, Double> presentedEdge = new Edge<String, Double>();
//                presentedEdge.setSource(title);
//                presentedEdge.setTarget(venue);
//                presentedEdge.setValue(1.0);
//                out.collect(presentedEdge);
//            }
//
//            // if keywords is not null or empty create edges (TAGGED)
////            if (keywords != null && keywords.size() > 0) {
////                for (String keyword: keywords){
////
////                    // create edge between paper -> keyword (TAGGED)
////                    Edge<String, PubEdgeValue> taggedEdge = new Edge<String, PubEdgeValue>();
////                    taggedEdge.setSource(title);
////                    taggedEdge.setTarget(keyword);
////                    taggedEdge.setValue(new PubEdgeValue(PubEdgeType.TAGGED, 1));
////                    out.collect(taggedEdge);
////                }
////            }
////
////            // if field of study is not not null or empty create edges (DOMAIN)
////            if (fos != null && fos.size() > 0) {
////                for (String field: fos){
////
////                    // create edge between paper -> field (DOMAIN)
////                    Edge<String, PubEdgeValue> domainEdge = new Edge<String, PubEdgeValue>();
////                    domainEdge.setSource(title);
////                    domainEdge.setTarget(field);
////                    domainEdge.setValue(new PubEdgeValue(PubEdgeType.DOMAIN, 1));
////                    out.collect(domainEdge);
////                }
////            }
//
//            // add edges between authors (COAUTHORED)
//            // add edges between author and paper (PUBLISHED)
//
//            // converts author set to array
//            String[] authorList = authors.toArray(new String[authors.size()]);
//
//            // create edge between 1st author -> paper (PUBLISHED)
//            Edge<String, Double> writtenEdge = new Edge<String, Double>();
//            writtenEdge.setSource(authorList[0]);
//            writtenEdge.setTarget(title);
//            writtenEdge.setValue(1.0);
//            out.collect(writtenEdge);
//
//            // more than one author worked on this publication
//            Integer totalAuthors = authorList.length;
//            if (totalAuthors > 1) {
//                for (int i = 0; i < authors.size() - 1; i++) {
//
//                    // get current author name
//                    String currentAuthor = authorList[i];
//
//                    // check if this author is not at position 0 and add published edge
//                    // create edge between other authors -> paper (PUBLISHED)
//                    if (i > 0) {
//                        Edge<String, Double> writtenOtherEdge = new Edge<String, Double>();
//                        writtenOtherEdge.setSource(currentAuthor);
//                        writtenOtherEdge.setTarget(title);
//                        writtenOtherEdge.setValue(1.0);
//                        out.collect(writtenOtherEdge);
//                    }
//
//                    for (int j = i + 1; j < totalAuthors; j++) {
//
//                        // gets co-author
//                        String coAuthor = authorList[j];
//
//                        // create edge [author -> co-author] (COAUTHORED)
//                        Edge<String, Double> coAuthoredEdge = new Edge<String, Double>();
//                        coAuthoredEdge.setSource(currentAuthor);
//                        coAuthoredEdge.setTarget(coAuthor);
//                        coAuthoredEdge.setValue(1.0);
//                        out.collect(coAuthoredEdge);
//                    }
//                }
//            }
//        }
//    }
//
//    private static final class PubVertexMapper2 implements FlatMapFunction<OagPublication, Vertex<String, Long>> {
//
//        @Override
//        public void flatMap(OagPublication publication, Collector<Vertex<String, Long>> out) throws Exception {
//
//            // gets information about the current publication
//
//            Set<String> fos = publication.getFos();
//
//            if (fos == null) {
//                return;
//            }
//
//            if (!fos.contains("computer science")) {
//                return;
//            }
//
//            // create vertex (PAPER)
//            String title = publication.getTitle();
//            out.collect(new Vertex<String, Long>(title, 1L));
//
//            // if publisher is not null or empty, create vertex (PUBLISHER)
//            String publisher = publication.getPublisher();
//            if (publisher != null && !publisher.isEmpty()) {
//                out.collect(new Vertex<String, Long>(publisher, 1L));
//            }
//
//            // if venue is not null or empty, create vertex (VENUE)
//            String venue = publication.getVenue();
//            if (venue != null && !venue.isEmpty()) {
//                out.collect(new Vertex<String, Long>(venue, 1L));
//            }
//
//            // add vertices for authors (AUTHOR)
//            Set<String> authors = publication.getAuthors();
//            for (String author : authors) {
//                out.collect(new Vertex<String, Long>(author, 1L));
//            }
//        }
//    }
//
//    private static final class PubEdgeMapper implements FlatMapFunction<OagPublication, Edge<String, PubEdgeValue>> {
//
//        @Override
//        public void flatMap(OagPublication publication, Collector<Edge<String, PubEdgeValue>> out) throws Exception {
//
//            /*
//             * TYPES OF EDGES
//             * --------------
//             * > Author -> Co-Author (COAUTHORED)
//             * > Author -> Paper (WRITTEN)
//             * > Paper  -> Publisher (PUBLISHED)
//             * > Paper  -> Venue (PRESENTED)
//             * > Paper  -> Keyword (TAGGED)
//             * > Paper  -> Field of study (DOMAIN)
//             * */
//
//            // gets information about the current publication
//            String title = publication.getTitle();
//            String publisher = publication.getPublisher();
//            String venue = publication.getVenue();
//            Set<String> keywords = publication.getKeywords();
//            Set<String> fos = publication.getFos();
//            Set<String> authors = publication.getAuthors();
//
//            if (fos == null) {
//                return;
//            }
//
//            if (!fos.contains("computer science")) {
//                return;
//            }
//
//            // if publisher is not null or empty create edge (PUBLISHED)
//            if (publisher != null && !publisher.isEmpty()) {
//
//                // create edge between publisher -> paper (PUBLISHED)
//                Edge<String, PubEdgeValue> publishedEdge = new Edge<String, PubEdgeValue>();
//                publishedEdge.setSource(publisher);
//                publishedEdge.setTarget(title);
//                publishedEdge.setValue(new PubEdgeValue(PubEdgeType.PUBLISHED, 1));
//                out.collect(publishedEdge);
//            }
//
//            // if venue is not null or empty create edge (PRESENTED)
//            if (venue != null && !venue.isEmpty()) {
//
//                // create edge between paper -> venue (PRESENTED)
//                Edge<String, PubEdgeValue> presentedEdge = new Edge<String, PubEdgeValue>();
//                presentedEdge.setSource(title);
//                presentedEdge.setTarget(venue);
//                presentedEdge.setValue(new PubEdgeValue(PubEdgeType.PRESENTED, 1));
//                out.collect(presentedEdge);
//            }
//
//            // if keywords is not null or empty create edges (TAGGED)
////            if (keywords != null && keywords.size() > 0) {
////                for (String keyword: keywords){
////
////                    // create edge between paper -> keyword (TAGGED)
////                    Edge<String, PubEdgeValue> taggedEdge = new Edge<String, PubEdgeValue>();
////                    taggedEdge.setSource(title);
////                    taggedEdge.setTarget(keyword);
////                    taggedEdge.setValue(new PubEdgeValue(PubEdgeType.TAGGED, 1));
////                    out.collect(taggedEdge);
////                }
////            }
////
////            // if field of study is not not null or empty create edges (DOMAIN)
////            if (fos != null && fos.size() > 0) {
////                for (String field: fos){
////
////                    // create edge between paper -> field (DOMAIN)
////                    Edge<String, PubEdgeValue> domainEdge = new Edge<String, PubEdgeValue>();
////                    domainEdge.setSource(title);
////                    domainEdge.setTarget(field);
////                    domainEdge.setValue(new PubEdgeValue(PubEdgeType.DOMAIN, 1));
////                    out.collect(domainEdge);
////                }
////            }
//
//            // add edges between authors (COAUTHORED)
//            // add edges between author and paper (PUBLISHED)
//
//            // converts author set to array
//            String[] authorList = authors.toArray(new String[authors.size()]);
//
//            // create edge between 1st author -> paper (PUBLISHED)
//            Edge<String, PubEdgeValue> writtenEdge = new Edge<String, PubEdgeValue>();
//            writtenEdge.setSource(authorList[0]);
//            writtenEdge.setTarget(title);
//            writtenEdge.setValue(new PubEdgeValue(PubEdgeType.WRITTEN, 1));
//            out.collect(writtenEdge);
//
//            // more than one author worked on this publication
//            Integer totalAuthors = authorList.length;
//            if (totalAuthors > 1) {
//                for (int i = 0; i < authors.size() - 1; i++) {
//
//                    // get current author name
//                    String currentAuthor = authorList[i];
//
//                    // check if this author is not at position 0 and add published edge
//                    // create edge between other authors -> paper (PUBLISHED)
//                    if (i > 0) {
//                        Edge<String, PubEdgeValue> writtenOtherEdge = new Edge<String, PubEdgeValue>();
//                        writtenOtherEdge.setSource(currentAuthor);
//                        writtenOtherEdge.setTarget(title);
//                        writtenOtherEdge.setValue(new PubEdgeValue(PubEdgeType.WRITTEN, 1));
//                        out.collect(writtenOtherEdge);
//                    }
//
//                    for (int j = i + 1; j < totalAuthors; j++) {
//
//                        // gets co-author
//                        String coAuthor = authorList[j];
//
//                        // create edge [author -> co-author] (COAUTHORED)
//                        Edge<String, PubEdgeValue> coAuthoredEdge = new Edge<String, PubEdgeValue>();
//                        coAuthoredEdge.setSource(currentAuthor);
//                        coAuthoredEdge.setTarget(coAuthor);
//                        coAuthoredEdge.setValue(new PubEdgeValue(PubEdgeType.COAUTHORED, 1));
//                        out.collect(coAuthoredEdge);
//                    }
//                }
//            }
//        }
//    }
//
//    private static final class PubVertexMapper implements FlatMapFunction<OagPublication, Vertex<String, PubVertexValue>> {
//
//        @Override
//        public void flatMap(OagPublication publication, Collector<Vertex<String, PubVertexValue>> out) throws Exception {
//
//            // gets information about the current publication
//
//            Set<String> fos = publication.getFos();
//
//            if (fos == null) {
//                return;
//            }
//
//            if (!fos.contains("computer science")) {
//                return;
//            }
//
//            // create vertex (PAPER)
//            String title = publication.getTitle();
//            out.collect(new Vertex<String, PubVertexValue>(
//                    title,
//                    new PubVertexValue(1L, PubVertexType.PAPER)));
//
//            // if publisher is not null or empty, create vertex (PUBLISHER)
//            String publisher = publication.getPublisher();
//            if (publisher != null && !publisher.isEmpty()) {
//                out.collect(new Vertex<String, PubVertexValue>(
//                        publisher,
//                        new PubVertexValue(1L, PubVertexType.PUBLISHER)));
//            }
//
//            // if venue is not null or empty, create vertex (VENUE)
//            String venue = publication.getVenue();
//            if (venue != null && !venue.isEmpty()) {
//                out.collect(new Vertex<String, PubVertexValue>(
//                        venue,
//                        new PubVertexValue(1L, PubVertexType.VENUE)));
//            }
//
//            // add vertices for authors (AUTHOR)
//            Set<String> authors = publication.getAuthors();
//            for (String author : authors) {
//                out.collect(new Vertex<String, PubVertexValue>(
//                        author,
//                        new PubVertexValue(1L, PubVertexType.AUTHOR)));
//            }
//        }
//    }
//}
