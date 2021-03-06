package batch;

/*
Handles/processes data in batch found in CassandraDB using Apache Flink (Community Detection).

Parameters
----------
> cassandra_point: the cassandra point (IP) that the driver uses to discover the cluster topology (local execution use 127.0.0.1)
> keywords: comma separated list for keywords input
> domains: comma separated list for domains input
> results_path: the path where to save all results
> total_sample_results: the number of samples to save as a sample
> sample_results_only: saves only sample results not full results "0" or "1"
> community_iterations: number of iterations used in the CommunityDetectionAlgorithm
> community_delta: delta used in the CommunityDetectionAlgorithm (default value 0.5)
> n_top_communities: the total top n communities to get results for
> n_dense_community: how many vertices with the same label to constitute as a dense community
*/

// importing packages

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.graph.library.CommunityDetection;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import publication.Publication;

import java.util.HashSet;
import java.util.Set;

public class ScipiBatchCommunity {

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

        // gets input for keywords (comma separated)
        final HashSet<String> definedKeywords = new HashSet<String>();
        if(parameters.get("keywords") != null) {
            final String[] keywords = parameters.get("keywords").split("\\s*,\\s*");

            // add keywords to keywords set
            for (String keyword : keywords) {
                definedKeywords.add(keyword.toLowerCase());
            }
        }

        // gets input for domains (comma separated)
        final HashSet<String> definedDomains = new HashSet<String>();
        if(parameters.get("domains") != null) {
            final String[] domains = parameters.get("domains").split("\\s*,\\s*");

            // add domain to domains set
            for (String domain : domains) {
                definedDomains.add(domain.toLowerCase());
            }
        }

        // gets inputs for results save only samples, paths, etc
        final String resultsPath = parameters.get("results_path");
        final Integer totalSamples = parameters.getInt("total_sample_results");
        final boolean saveOnlySample = parameters.get("sample_results_only").equals("1");

        // gets inputs for CommunityDetectionAlgorithm
        // get input for number of iterations used in community detection algorithm
        Integer communityIterations = parameters.getInt("community_iterations");

        // get input for delta used in community detection algorithm
        Double communityDelta = parameters.getDouble("community_delta");

        // get the total n top communities to get results for
        Integer totalTopCommunities = parameters.getInt("n_top_communities");

        // how many vertices with the same label to constitute as a dense community
        final Integer nDenseCommunity = parameters.getInt("n_dense_community");

        // create POJO input format to get Publication entities from CassandraDB
        CassandraPojoInputFormat cassandraPojoInputFormat = new CassandraPojoInputFormat<Publication>(
                "SELECT * FROM scipi.publications;",
                cassandraBuilder,
                Publication.class
        );

        // we need to specify TypeInformation for Publication POJO
        TypeInformation<Publication> typeInformation = TypeInformation.of(new TypeHint<Publication>() {
        });

        // retrieve Publication entities as a dataset from Cassandra
        DataSet<Publication> publications = environment.createInput(cassandraPojoInputFormat, typeInformation);

        // create filter to get publications which have
        // at least one keyword or domain which was passed as a parameter
        DataSet<Publication> filteredPublications = publications
                .filter(new FilterFunction<Publication>() {
                    @Override
                    public boolean filter(Publication publication) throws Exception {

                        // check if it contains at least one keyword from input
                        Set<String> publicationKeywords = publication.getKeywords();
                        if (publicationKeywords != null && publicationKeywords.size() > 0) {
                            for (String keyword : publicationKeywords) {
                                if (definedKeywords.contains(keyword)) {
                                    return true;
                                }
                            }
                        }

                        // check if it contains at least one domain from input
                        Set<String> publicationDomains = publication.getFos();
                        if (publicationDomains != null && publicationDomains.size() > 0) {
                            for (String domain : publicationDomains) {
                                if (definedDomains.contains(domain)) {
                                    return true;
                                }
                            }
                        }

                        return false; // no keyword or domain intersected
                    }
                });

        // Community Detection /////////////////////////////////////////////////
        // create one big graph which holds all entities in publication network

        // create vertices, map Publication to Vertex<String, PubVertexValue>
        DataSet<Vertex<String, PubVertexValue>> networkVertices = filteredPublications
                .flatMap(new NetworkVertexMapper())
                .distinct(0); // only unique vertices by name

        // create edges, map OagPublication to Edge<String, Double>
        DataSet<Edge<String, Double>> networkEdges = filteredPublications
                .flatMap(new NetworkEdgeMapper());

        // create an undirected publication network graph
        Graph<String, PubVertexValue, Double> networkGraph = Graph
                .fromDataSet(networkVertices, networkEdges, environment);

        // create dataset made up of (VertexId, UniqueLabel)
        // these unique labels will be the initial values for each vertex when applying
        // the CommunityDetection algorithm
        DataSet<Tuple2<String, Long>> vertexIdsInitialLabels = DataSetUtils
                .zipWithUniqueId(networkGraph.getVertexIds())
                .map(new MapFunction<Tuple2<Long, String>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple2<Long, String> value) throws Exception {
                        return new Tuple2<String, Long>(value.f1, value.f0);
                    }
                });

        // > firstly we translate the original network graph by translating the vertex value
        // to Long, this is done since CommunityDetection algorithm requires that vertex value is of type Long
        // > after doing so, the vertices are joined with the dataset <VertexId, UniqueLabel> to set
        // a unique label for each vertex, then the CommunityDetection algorithm is called
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
                }).run(new CommunityDetection<String>(communityIterations, communityDelta));

        // - now we will get the top three communities/labels after the algorithm is finished
        // > map vertices to (Label, 1)
        // > group by label
        // > sum on second value in the tuple
        // > we define a community if it has at 500 vertices with the same label, filtered by count
        DataSet<Tuple2<Long, Long>> communityLabelsCount = communityGraph
                .getVertices()
                .map(new MapFunction<Vertex<String, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Vertex<String, Long> vertex) throws Exception {

                        // emit (CommunityLabel, 1)
                        return new Tuple2<Long, Long>(vertex.f1, 1L);
                    }
                })
                .groupBy(0) // group by CommunityLabel
                .sum(1) // sum all occurrences
                .filter(new FilterFunction<Tuple2<Long, Long>>() {
                    @Override
                    public boolean filter(Tuple2<Long, Long> value) throws Exception {
                        return value.f1 >= nDenseCommunity;
                    }
                });

        // save the community label count
        communityLabelsCount.writeAsCsv(resultsPath + "/communityLabelCount.csv",
                                        FileSystem.WriteMode.OVERWRITE);

        // get the most dense communities/labels
        final HashSet<Long> denseLabels = new HashSet<Long>(communityLabelsCount
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .first(totalTopCommunities)
                .map(new MapFunction<Tuple2<Long, Long>, Long>() {
                    @Override
                    public Long map(Tuple2<Long, Long> value) throws Exception {
                        return value.f0;
                    }
                }).collect());

        // after applying CommunityDetection we need to translate back the Vertex value to PubVertexValue
        // > filter only vertices which are part of the top three communities
        // > translate vertex value from Long to PubVertexValue and set type to NONE
        // > then join on the original graph and set the translated vertex value type to the initial one
        Graph<String, PubVertexValue, Double> denseCommunityGraph = communityGraph
                .filterOnVertices(new FilterFunction<Vertex<String, Long>>() {
                    @Override
                    public boolean filter(Vertex<String, Long> vertex) throws Exception {
                        return denseLabels.contains(vertex.f1);
                    }
                })
                .translateVertexValues(new TranslateFunction<Long, PubVertexValue>() {
                    @Override
                    public PubVertexValue translate(Long communityLabel, PubVertexValue o) throws Exception {
                        return new PubVertexValue(communityLabel, PubVertexType.NONE);
                    }
                }).joinWithVertices(networkGraph.getVerticesAsTuple2(), new VertexJoinFunction<PubVertexValue, PubVertexValue>() {
                    @Override
                    public PubVertexValue vertexJoin(PubVertexValue communityVertex,
                                                     PubVertexValue originalVertex) throws Exception {
                        originalVertex.setVertexValue(communityVertex.getVertexValue());
                        return originalVertex;
                    }
                });

        // get vertices which are part of the top three communities
        DataSet<Tuple3<String, String, Long>> denseCommunityVertices = denseCommunityGraph
                .getVertices()
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

        // get edges which are part of the vertices found in the top three communities
        DataSet<Tuple2<String, String>> denseCommunityEdges = denseCommunityGraph
                .getEdges()
                .map(new MapFunction<Edge<String, Double>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Edge<String, Double> edge) throws Exception {
                        return new Tuple2<String, String>(edge.getSource(), edge.getTarget());
                    }
                }).distinct();

        // save CommunityDetection results

        // saves only sample
        if (saveOnlySample) {

            // get result sample result only (name, type, label, name, type, label)
            DataSet<Tuple6<String, String, Long, String, String, Long>> denseCommunityVerticesEdges =
                    denseCommunityEdges.first(totalSamples)
                            .join(denseCommunityVertices)
                            .where(0)
                            .equalTo(0)
                            .projectSecond(0, 1, 2).projectFirst(1)
                            .join(denseCommunityVertices)
                            .where(3)
                            .equalTo(0)
                            .projectFirst(0, 1, 2).projectSecond(0, 1, 2);

            // save to specified path
            denseCommunityVerticesEdges.writeAsCsv(
                    resultsPath + "/communitySample.csv",
                    FileSystem.WriteMode.OVERWRITE // overwrite existing file if exists
            );
        }
        // saves both full result and sample
        else {

            // get full result
            DataSet<Tuple6<String, String, Long, String, String, Long>> denseCommunityVerticesEdges =
                    denseCommunityEdges
                            .join(denseCommunityVertices)
                            .where(0)
                            .equalTo(0)
                            .projectSecond(0, 1, 2).projectFirst(1)
                            .join(denseCommunityVertices)
                            .where(3)
                            .equalTo(0)
                            .projectFirst(0, 1, 2).projectSecond(0, 1, 2);

            // save full result to specified path
            denseCommunityVerticesEdges.writeAsCsv(
                    resultsPath + "/communityFull.csv",
                    FileSystem.WriteMode.OVERWRITE // overwrite existing file if exists
            );

            // save sample result to specified path
            denseCommunityVerticesEdges.first(totalSamples)
                    .writeAsCsv(
                            resultsPath + "/communitySample.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if exists
                    );
        }

        // END Community Detection /////////////////////////////////////////////////

        // execute job
        environment.execute("scipi Community Detection");
    }

    /***************************************************
     USER DEFINED FUNCTIONS
     **************************************************/

    private static Edge<String, Double> CreateEdge(String source, String target, Double value) {
        Edge<String, Double> edge = new Edge<String, Double>();
        edge.setSource(source);
        edge.setTarget(target);
        edge.setValue(value);
        return edge;
    }

    private static final class NetworkVertexMapper implements FlatMapFunction<Publication, Vertex<String, PubVertexValue>> {

        @Override
        public void flatMap(Publication publication,
                            Collector<Vertex<String, PubVertexValue>> out) throws Exception {

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

    private static final class NetworkEdgeMapper implements FlatMapFunction<Publication, Edge<String, Double>> {

        @Override
        public void flatMap(Publication publication,
                            Collector<Edge<String, Double>> out) throws Exception {

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
