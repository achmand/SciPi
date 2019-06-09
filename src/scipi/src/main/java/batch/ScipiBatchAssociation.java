package batch;

/*
Handles/processes data in batch found in CassandraDB using Apache Flink (Association/Correlation Analysis.

Parameters
----------
> cassandra_point: the cassandra point (IP) that the driver uses to discover the cluster topology (local execution use 127.0.0.1)
> keywords: comma separated list for keywords input
> results_path: the path where to save all results
> total_sample_results: the number of samples to save as a sample
> sample_results_only: saves only sample results not full results "0" or "1"
> cosine_k: vectors of occurrences of k-shingles (sequences of k characters) for cosine algorithm
> cosine_similarity_threshold: the similarity threshold which constitutes as an association
> kw_usage_threshold: author to keyword association -> threshold to get only strong associations
*/

// importing packages

import com.datastax.driver.core.Cluster;
import info.debatty.java.stringsimilarity.Cosine;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.batch.connectors.cassandra.CassandraPojoInputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.bipartite.BipartiteEdge;
import org.apache.flink.graph.bipartite.BipartiteGraph;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import publication.Publication;

import java.util.HashSet;
import java.util.Set;

public class ScipiBatchAssociation {

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
        if (parameters.get("keywords") != null) {
            final String[] keywords = parameters.get("keywords").split("\\s*,\\s*");

            // add keywords to keywords set
            for (String keyword : keywords) {
                definedKeywords.add(keyword.toLowerCase());
            }
        }

        // gets inputs for results save only samples, paths, etc
        final String resultsPath = parameters.get("results_path");
        final Integer totalSamples = parameters.getInt("total_sample_results");
        final boolean saveOnlySample = parameters.get("sample_results_only").equals("1");

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

        // get cosine similarity parameters
        final Integer cosineK = parameters.getInt("cosine_k");
        final double cosineThreshold = parameters.getDouble("cosine_similarity_threshold");

        // define & discover associations between authors and keywords (taken from the title)
        final Cosine cosineSimilarity = new Cosine(cosineK);

        DataSet<Tuple3<String, String, Double>> authorKeywordAssociation = publications
                .flatMap(new FlatMapFunction<Publication, Tuple3<String, String, Double>>() {

                    @Override
                    public void flatMap(Publication publication,
                                        Collector<Tuple3<String, String, Double>> out) throws Exception {

                        // get title from publication
                        String title = publication.getTitle();

                        // get publication authors
                        Set<String> authors = publication.getAuthors();

                        for (String keyword : definedKeywords) {

                            // compute cosine similarity between keyword and paper title
                            double similarity = cosineSimilarity.similarity(keyword, title);

                            // check if it over the threshold
                            if (similarity > cosineThreshold) {

                                // loop in each author
                                for (String author : authors) {
                                    out.collect(new Tuple3<String, String, Double>(keyword, author, similarity));
                                }
                            }
                        }
                    }
                }).groupBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2 getKey(Tuple3<String, String, Double> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                }).reduce(new ReduceFunction<Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> reduce(
                            Tuple3<String, String, Double> current,
                            Tuple3<String, String, Double> pre) throws Exception {
                        return new Tuple3<String, String, Double>(current.f0, current.f1, current.f2 + pre.f2);
                    }
                });

        // saves only sample
        if (saveOnlySample) {

            // save to specified path
            authorKeywordAssociation
                    .first(totalSamples)
                    .writeAsCsv(
                            resultsPath + "/authorKwSample.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );
        }
        // saves both full result and sample
        else {

            // save to specified path
            authorKeywordAssociation
                    .writeAsCsv(
                            resultsPath + "/authorKwFull.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );

            // save to specified path
            authorKeywordAssociation
                    .first(totalSamples)
                    .writeAsCsv(
                            resultsPath + "/authorKwSample.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );
        }

        // threshold to get only strong associations
        final Integer kwThreshold = parameters.getInt("kw_usage_threshold");

        // perform clustering of authors based on their association to keywords
        // so to recommend potential collaborators
        DataSet<BipartiteEdge<String, String, Integer>> authorKeywordsEdges = publications
                .flatMap(new FlatMapFunction<Publication, Tuple3<String, String, Integer>>() {
                    @Override
                    public void flatMap(Publication publication,
                                        Collector<Tuple3<String, String, Integer>> out) throws Exception {

                        // get keywords from publication
                        Set<String> keywords = publication.getKeywords();

                        // since we will perform clustering based on keywords
                        // if keyword set is empty move on
                        if (keywords == null || keywords.isEmpty()) {
                            return;
                        }

                        // intersect defined keyword set and keywords found in this publication
                        // we need to create a new HashSet since retainAll() will remove any elements
                        // which are not found in the second collection
                        Set<String> intersection = new HashSet<String>(definedKeywords);
                        intersection.retainAll(keywords);

                        // check if the intersection set contains any elements
                        if (intersection.isEmpty()) {
                            return; // publication does not contain keywords which were specified
                        }

                        // get the authors set
                        Set<String> authors = publication.getAuthors();
                        for (String author : authors) {
                            for (String keyword : intersection) {

                                // emit (Author, Keyword, 1)
                                out.collect(new Tuple3<String, String, Integer>(author, keyword, 1));
                            }
                        }
                    }
                }).groupBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2 getKey(Tuple3<String, String, Integer> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                }).reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> current,
                                                                  Tuple3<String, String, Integer> pre) throws Exception {

                        return new Tuple3<String, String, Integer>(current.f0, current.f1, current.f2 + pre.f2);
                    }
                }).filter(new FilterFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> edge) throws Exception {
                        return (edge.f2 > kwThreshold);
                    }
                }).map(new MapFunction<Tuple3<String, String, Integer>,
                                        BipartiteEdge<String, String, Integer>>() {
                    @Override
                    public BipartiteEdge<String, String, Integer> map(
                            Tuple3<String, String, Integer> value) throws Exception {
                        return new BipartiteEdge<String, String, Integer>(value.f0, value.f1, value.f2);
                    }
                });

        // create vertices for top section of a bipartite graph (Authors)
        DataSet<Vertex<String, NullValue>> topAuthorVertices = authorKeywordsEdges.
                map(new MapFunction<BipartiteEdge<String, String, Integer>, Vertex<String, NullValue>>() {
                    @Override
                    public Vertex<String, NullValue> map(BipartiteEdge<String, String, Integer> edge) throws Exception {
                        Vertex<String, NullValue> v = new Vertex<String, NullValue>();
                        v.setId(edge.getTopId());
                        return v;
                    }
                }).distinct();

        // create vertices for bottom section of a bipartite graph (Keywords)
        DataSet<Vertex<String, NullValue>> bottomKeywordVertices = environment.fromCollection(definedKeywords)
                .map(new MapFunction<String, Vertex<String, NullValue>>() {
                    @Override
                    public Vertex<String, NullValue> map(String value) throws Exception {
                        Vertex<String, NullValue> v = new Vertex<String, NullValue>();
                        v.setId(value);
                        return v;
                    }
                });

        // create bipartite graph Authors to Keywords
        BipartiteGraph<String, String, NullValue, NullValue, Integer> authorsKeywordGraph = BipartiteGraph
                .fromDataSet(topAuthorVertices, bottomKeywordVertices, authorKeywordsEdges, environment);

        // get top section of graph
        Graph<String, NullValue, Tuple2<Integer, Integer>> similarAuthorsGraph = authorsKeywordGraph
                .projectionTopSimple();

        // create edges between these authors as tuples
        DataSet<Tuple2<String, String>> similarAuthorsEdges = similarAuthorsGraph
                .getEdgesAsTuple3()
                .map(new MapFunction<Tuple3<String, String, Tuple2<Integer, Integer>>,
                        Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(Tuple3<String, String, Tuple2<Integer, Integer>>
                                                              value) throws Exception {
                        return new Tuple2<String, String>(value.f0, value.f1);
                    }
                });

        // saves only sample
        if (saveOnlySample) {

            // save to specified path
            similarAuthorsEdges
                    .first(totalSamples)
                    .writeAsCsv(
                            resultsPath + "/authorsCollabSample.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );
        }
        // saves both full result and sample
        else {

            // save to specified path
            similarAuthorsEdges
                    .writeAsCsv(
                            resultsPath + "/authorsCollabFull.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );

            // save to specified path
            similarAuthorsEdges
                    .first(totalSamples)
                    .writeAsCsv(
                            resultsPath + "/authorsCollabSample.csv",
                            FileSystem.WriteMode.OVERWRITE // overwrite existing file if
                    );
        }

        // execute job
        environment.execute("scipi Association/Correlation");
    }
}
