package stream;

/*

Handles/processes Kafka streams which contains publications using Apache Flink.

-------------------
CASSANDRA DB TABLES
-------------------
- Keyspace: scipi
- Tables
    > oagpub: publications coming from OAG [doi, title, publisher, venue, lang, keywords, year, authors]
    > oagkw:  keyword count from OAG publications [keyword, count]
    > oagfos: field of study count from OAG publications [fos, count]
    > yrwisedist: single vs co-authored year wise distributions
                  [year, total single, total co-authored, total publications,
                  percentage single author, percentage co-authored]

-------------
PROCESS FLOW
-------------
- 0.0: consume data stream from kafka

- 1.0: map json strings passed from kafka to flink stream (POJO per publication)
      > 1.0.1: only publications written in english
      > 1.0.2: doi must not be empty as it is used as an id in CassandraDB
      > 1.0.3: title must not be empty
      > 1.0.4: at least a publisher or venue
      > 1.0.5: at least one keyword or field of study
          > 1.0.5.1: clean keywords and keep only valid ones
            > 1.0.5.2: clean fos and keep only valid ones
      > 1.0.6: must have a valid year
      > 1.0.7: must have at least one author
            > 1.0.7.1: clean authors and keep only valid ones

- 1.1: persist publications to CassandraDB using data sink

- 2.0: [keyword count]: (keyword, count)
                      - map OagPublication to (keyword, 1)
                      - key by keyword
                      - sum keyword on count

- 2.1: persist [keyword count] to CassandraDB using data sink

- 3.0: [field of study count]: (fos, count)
                              - map OagPublication to (fos, 1)
                              - key by fos
                              - sum fos on count

- 3.1: persist [field of study count count] to CassandraDB using data sink

- 4.0: [year wise distribution]: (yr, tot single, tot joint, tot, %single, %joint)
                              - map OagPublication to (yr, single, joint) -> using YearWiseMapper:map
                              - key by year
                              - reduce (yr, single, joint) to (yr, tot single, tot joint)
                                  -> using YearWiseReducer:reduce
                              - map (yr, tot single, tot joint) to (yr, tot single, tot joint, tot, %single, %joint)
                                  -> using YearPercMapper:map

- 4.1: persist [year wise distribution] to CassandraDB using data sink

- 5.0: [authorship patterns]: (no. authors, no. publications, tot authors, %publications)

*/

// importing packages

import com.datastax.driver.core.Cluster;
import com.datastax.driver.mapping.Mapper;
import com.google.gson.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import publication.OagPublication;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;


// TODO -> Publisher check length

public class ScipiStream {

    // used to parse JSON to POJO
    private final static Gson gson = new GsonBuilder()
            .registerTypeAdapter(OagPublication.class, new PubDeserializer())
            .create();

    public static void main(String[] args) throws Exception {

        // returns the stream execution environment (the context 'Local or Remote' in which a program is executed)
        // LocalEnvironment will cause execution in the current JVM
        // RemoteEnvironment will cause execution on a remote setup
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // provides utility methods for reading and parsing the program arguments
        // in this tutorial we will have to provide the input file and the output file as arguments
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        // register parameters globally so it can be available for each node in the cluster
        environment.getConfig().setGlobalJobParameters(parameters);

        // set properties for kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // IP address where Kafka is running

        // set properties for cassandra
        ClusterBuilder cassandraBuilder = new ClusterBuilder() {
            @Override
            public Cluster buildCluster(Cluster.Builder builder) {
                return builder.addContactPoint("127.0.0.1")
                        .build();
            }
        };

        // 0.0: consume data stream from kafka
        DataStream<String> kafkaData = environment.addSource(
                new FlinkKafkaConsumer<String>("oag", new SimpleStringSchema(), properties));

        // 1.0: first we map strings from Kafka to OagPublication using OagPubMapper
        DataStream<OagPublication> oagPublications = kafkaData.flatMap(new OagPubMapper());

        // 1.1: persist publications to CassandraDB using data sink
        CassandraSink.addSink(oagPublications)
                .setClusterBuilder(cassandraBuilder)
                .setMapperOptions(new MapperOptions() {
                    @Override
                    public Mapper.Option[] getMapperOptions() {
                        return new Mapper.Option[]{Mapper.Option.saveNullFields(true)};
                    }
                }).build();

        // 2.0: map OagPublication to (keyword, count)
        DataStream<Tuple2<String, Integer>> oagKeywords = oagPublications
                .flatMap(new OagKwMapper()) // map
                .keyBy(0)           // key by keyword
                .sum(1);       // sum the emitted 1

        // 2.1: persist occurrences count for keyword to CassandraDB using data sink
        CassandraSink.addSink(oagKeywords)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.oagkw(keyword, count) values (?, ?);")
                .build();

        // 3.0: map OagPublication to (fos, count)
        DataStream<Tuple2<String, Integer>> oagFos = oagPublications
                .flatMap(new OagFosMapper())    // map
                .keyBy(0)               // key by field of study
                .sum(1);           // sum the emitted 1

        // 3.1: persist occurrences count for fos to CassandraDB using data sink
        CassandraSink.addSink(oagFos)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.oagfos(fos, count) values (?, ?);")
                .build();

        // 4.0: map OagPublication to (yr, tot single, tot co-authored, tot pub, %single, %co-authored)
        DataStream<Tuple6<String, Integer, Integer, Integer, Double, Double>> yrWiseDist = oagPublications
                .map(new YearWiseMapper())       // map
                .keyBy(0)                // key by year published
                .reduce(new YearWiseReducer())   // reduce by counting single & co-authored
                .map(new YearPercMapper());

        // 4.1: persist year-wise distribution to CassandraDB using data sink
        CassandraSink.addSink(yrWiseDist)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.yrwisedist(year, single, joint, total, single_perc, joint_perc)" +
                        " values (?, ?, ?, ?, ?, ?);")
                .build();

        // 5.0: map OagPublication to (no. authors, no. publications, tot authors)
        DataStream<Tuple3<Integer, Integer, Integer>> authorshipPattern = oagPublications
                .map(new AuthorshipMapper())      // map  (no. authors, no. articles, tot no. authors)
                .keyBy(0)                 // key by no. authors (unit)
                .reduce(new AuthorshipReducer()); // reduce by adding up total publications and total authors

        // 5.1: persist authorship patterns to CassandraDB using data sink
        CassandraSink.addSink(authorshipPattern)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.authorptrn(author_unit, no_articles, no_authors) values (?, ?, ?);")
                .build();

        // 6.0: map OagPublication to (year, no. authors, no. publications, avg no authors per paper AAP)
        DataStream<Tuple4<String, Integer, Integer, Double>> aap = oagPublications
                .map(new AapMapper())
                .keyBy(0)
                .reduce(new AapReducer())
                .map(new AapAvgMapper());

        // 6.1: persist AAP to CassandraDB using data sink
        CassandraSink.addSink(aap)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.aap(year, no_authors, no_articles, avg_author_paper) values (?, ?, ?, ?);")
                .build();

        // execute stream processing
        environment.execute("scipi stream processing");
    }

    /***************************************************
     USER DEFINED FUNCTIONS
     **************************************************/

    // validates string attributes
    private static String validateStr(String str) {
        if (str == null) {
            return null;
        }

        // keep only letters, numbers and spaces
        str = str.replaceAll("[^a-zA-Z0-9\\s]", "").trim();
        if (str.isEmpty()) {
            return null;
        }

        return str.toLowerCase();
    }

    // validates topics
    private static Set<String> validateTopics(Set<String> topics) {

        Set<String> vTopics = new HashSet<String>();
        for (String topic : topics) {
            topic = validateStr(topic);

            // do not accept empty topic or topic with more than 30 char
            if (topic == null || topic.length() > 30) {
                continue;
            }

            // keep only letters, numbers and spaces
            topic = topic.replaceAll("[^a-zA-Z0-9\\s]", "").trim();
            if (topic.isEmpty()) {
                continue;
            }

            // append cleaned topic
            if (!vTopics.contains(topic)) {
                vTopics.add(topic);
            }
        }

        return vTopics;
    }

    // OagPublication JSON deserializer
    private static class PubDeserializer implements JsonDeserializer<OagPublication> {

        @Override
        public OagPublication deserialize(JsonElement jsonElement,
                                          Type type,
                                          JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {

            // get json object
            JsonObject jsonObject = jsonElement.getAsJsonObject();

            // get keywords
            HashSet<String> keywords = null;

            // check that json array is not empty
            if (jsonObject.get("keywords") != null) {
                JsonArray jsonKeywords = jsonObject.get("keywords").getAsJsonArray();
                if (jsonKeywords.size() > 0) {
                    keywords = new HashSet<String>(jsonKeywords.size());
                    for (JsonElement keyword : jsonKeywords) {
                        if (!keywords.contains(keyword)) {
                            keywords.add(keyword.getAsString());
                        }
                    }
                }
            }

            // get authors
            HashSet<String> authors = null;

            // check that json array is not empty
            if (jsonObject.get("authors") != null) {
                JsonArray jsonAuthors = jsonObject.get("authors").getAsJsonArray();
                if (jsonAuthors.size() > 0) {
                    authors = new HashSet<String>(jsonAuthors.size());
                    for (JsonElement author : jsonAuthors) {
                        String authorName = author.getAsJsonObject().get("name").getAsString();
                        if (!authors.contains(authorName)) {
                            authors.add(authorName);
                        }
                    }
                }
            }

            // get fos
            HashSet<String> fos = null;

            // check that json array is not empty
            if (jsonObject.get("fos") != null) {
                JsonArray jsonFos = jsonObject.get("fos").getAsJsonArray();
                if (jsonFos.size() > 0) {
                    fos = new HashSet<String>(jsonFos.size());
                    for (JsonElement field : jsonFos) {
                        if (!fos.contains(field)) {
                            fos.add(field.getAsString());
                        }
                    }
                }
            }

            String doi = null;
            if (jsonObject.get("doi") != null) {
                doi = jsonObject.get("doi").getAsString();
            }

            String title = null;
            if (jsonObject.get("title") != null) {
                title = jsonObject.get("title").getAsString();
            }

            String publisher = null;
            if (jsonObject.get("publisher") != null) {
                publisher = jsonObject.get("publisher").getAsString();
            }

            String venue = null;
            if (jsonObject.get("venue") != null) {
                venue = jsonObject.get("venue").getAsString();
            }

            String lang = null;
            if (jsonObject.get("lang") != null) {
                lang = jsonObject.get("lang").getAsString();
            }

            String year = null;
            if (jsonObject.get("year") != null) {
                year = jsonObject.get("year").getAsString();
            }

            // return publication
            return new OagPublication(
                    doi,
                    title,
                    publisher,
                    venue,
                    lang,
                    keywords,
                    year,
                    authors,
                    fos);
        }
    }

    // mapper: string to POJO (OagPublication)
    private static final class OagPubMapper implements FlatMapFunction<String, OagPublication> {

        @Override
        public void flatMap(String value, Collector<OagPublication> out) throws Exception {

            // parse string/json to OagPublication
            OagPublication publication = gson.fromJson(value, OagPublication.class);

            // 1.0.1: validate language
            String lang = validateStr(publication.getLang());

            // do not accept empty language
            if (lang == null) {
                return;
            }

            // do not accept non english publications
            if (!lang.equals("en")) {
                return;
            }

            // 1.0.2: validate doi
            String doi = validateStr(publication.getDoi());

            // do not accept empty doi
            if (doi == null) {
                return;
            }

            // set to trimmed/lowercase doi
            publication.setDoi(doi);

            // 1.0.3: validate title
            String title = validateStr(publication.getTitle());

            // do not accept empty title
            if (title == null) {
                return;
            }

            // set to trimmed/lowercase title
            publication.setTitle(title);

            // 1.0.4: validate publisher and venue
            String publisher = validateStr(publication.getPublisher());
            String venue = validateStr(publication.getVenue());

            // must have at least publisher or venue
            if (publisher == null && venue == null) {
                return;
            }

            // if publisher not empty set trimmed/lowercase publisher
            if (publisher != null) {
                publication.setPublisher(publisher);
            }

            // if venue not empty set to trimmed/lowercase venue
            if (venue != null) {
                publication.setVenue(venue);
            }

            // 1.0.5: validate keywords and field of study
            Set<String> keywords = publication.getKeywords();
            boolean validKeywords = keywords != null && keywords.size() > 0;

            Set<String> fos = publication.getFos();
            boolean validFos = fos != null && fos.size() > 0;

            // must have at least a keyword or field of study
            if (!validKeywords && !validFos) {
                return;
            }

            // 1.0.5.1: clean keywords and keep only valid ones
            Set<String> vKeywords = validKeywords ? validateTopics(keywords) : null;

            // 1.0.5.2: clean fos and keep only valid ones
            Set<String> vFos = validFos ? validateTopics(fos) : null;

            // check that at least some keywords or fos remain after cleaned
            if ((vKeywords == null || vKeywords.size() <= 0) && (vFos == null || vFos.size() <= 0)) {
                return;
            }

            // set to cleaned keywords
            publication.setKeywords(vKeywords);

            // set to cleaned fields of study
            publication.setFos(vFos);

            // 1.0.6: validate year
            String year = validateStr(publication.getYear());

            // do not accept empty year or invalid year
            if (year == null || year.length() != 4) {
                return;
            }

            // 1.0.6: validate authors
            Set<String> authors = publication.getAuthors();
            boolean validAuthors = authors != null && authors.size() > 0;

            // must have at least one author
            if (!validAuthors) {
                return;
            }

            // 1.0.7.1: clean authors and keep only valid ones
            Set<String> vAuthors = new HashSet<String>();
            for (String author : authors) {
                author = validateStr(author);

                // do not accept empty author
                if (author == null) {
                    continue;
                }

                // keep only letters, numbers and spaces
                author = author.replaceAll("[^a-zA-Z0-9\\s]", "");
                if (author.isEmpty()) {
                    continue;
                }

                // append cleaned author name
                if (!vAuthors.contains(author)) {
                    vAuthors.add(author);
                }
            }

            // check that at least some authors remain after cleaned
            if (vAuthors.size() <= 0) {
                return;
            }

            // set to cleaned authors
            publication.setAuthors(vAuthors);

            // collect publication
            out.collect(publication);
        }
    }

    // mapper: OagPublication to Tuple<String, int> to count occurrences (keywords)
    private static final class OagKwMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {

        @Override
        public void flatMap(OagPublication value, Collector<Tuple2<String, Integer>> out) throws Exception {

            // get keyword set from OagPublication
            Set<String> keywords = value.getKeywords();

            // check if keywords set is valid
            if (keywords != null && keywords.size() > 0) {

                // no need to validate since OagPublication was validated at an early stage
                // map keyword to => (keyword : 1)
                for (String keyword : keywords) {

                    // emit (keyword : 1)
                    out.collect(new Tuple2<String, Integer>(keyword, 1));
                }
            }
        }
    }

    // mapper: OagPublication to Tuple<String, int> to count occurrences (field of study)
    private static final class OagFosMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {

        @Override
        public void flatMap(OagPublication value, Collector<Tuple2<String, Integer>> out) throws Exception {

            // get fos set from OagPublication
            Set<String> fos = value.getFos();

            // check if fos set is valid)
            if (fos != null && fos.size() > 0) {

                // no need to validate since OagPublication was validated at an early stage
                // map keyword to => (field : 1)
                for (String field : fos) {

                    // emit (fos : 1)
                    out.collect(new Tuple2<String, Integer>(field, 1));
                }
            }
        }
    }

    // mapper: OagPublication to Tuple<String, int> to count occurrences (single vs co-authored publications)
    private static class YearWiseMapper implements MapFunction<OagPublication, Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> map(OagPublication publication) throws Exception {

            // get year from OagPublication
            String year = publication.getYear();

            // get authors from OagPublication
            Set<String> authors = publication.getAuthors();

            // set single and joint authored
            Integer authorsCount = authors.size();
            Integer single = authorsCount == 1 ? 1 : 0;
            Integer joint = authorsCount > 1 ? 1 : 0;

            // emit (year, single, joint)
            return new Tuple3<String, Integer, Integer>(year, single, joint);
        }
    }

    // reducer: reduce by adding up single-author and co-authored publications
    private static class YearWiseReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
                                                       Tuple3<String, Integer, Integer> pre) throws Exception {

            // emit reduced tuple (year, single, joint)
            return new Tuple3<String, Integer, Integer>(current.f0, current.f1 + pre.f1, current.f2 + pre.f2);
        }
    }

    // mapper: maps (yr, single, co-authored) to (yr, single, co-authored, %single, %co-authored)
    private static class YearPercMapper implements MapFunction<Tuple3<String, Integer, Integer>,
            Tuple6<String, Integer, Integer, Integer, Double, Double>> {

        @Override
        public Tuple6<String, Integer, Integer, Integer, Double, Double> map(
                Tuple3<String, Integer, Integer> value) throws Exception {

            // sum total single and co-authored to get total publications
            Integer totalPub = value.f1 + value.f2;

            // return (yr, tot single, tot co-authored, tot pub, %single, %co-authored)
            return new Tuple6<String, Integer, Integer, Integer, Double, Double>(
                    value.f0, // year
                    value.f1, // single author
                    value.f2, // co-authored
                    totalPub, // total publications
                    new Double((value.f1 * 1.0) / totalPub), // percentage single author
                    new Double((value.f2 * 1.0) / totalPub)  // percentage co-authored
            );
        }
    }

    // mapper: maps OagPublication to (no. authors, no. articles, tot no. authors)
    private static class AuthorshipMapper implements MapFunction<OagPublication,
            Tuple3<Integer, Integer, Integer>> {

        @Override
        public Tuple3<Integer, Integer, Integer> map(OagPublication publication) throws Exception {

            // no of authors who worked on this publication (unit)
            Integer noAuthors = publication.getAuthors().size();

            // return (no. authors, no. articles, tot no.authors)
            return new Tuple3<Integer, Integer, Integer>(noAuthors, 1, noAuthors);
        }
    }

    // reducer: reduce by adding up total publications and total authors
    private static class AuthorshipReducer implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {

        @Override
        public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> current,
                                                        Tuple3<Integer, Integer, Integer> prev) throws Exception {

            // sum up total publications
            Integer totPublications = current.f1 + prev.f1;

            // total publications * no.authors (unit)
            Integer totAuthors = totPublications * current.f0;

            // emit reduced tuple (no. authors, no. articles, tot no.authors)
            return new Tuple3<Integer, Integer, Integer>(current.f0, totPublications, totAuthors);
        }
    }

    // mapper: maps OagPublication to (year, no. authors, no. publications)
    private static class AapMapper implements MapFunction<OagPublication, Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> map(OagPublication publication) throws Exception {

            // get year from OagPublication
            String year = publication.getYear();

            // get authors from OagPublication
            Set<String> authors = publication.getAuthors();

            // get total number of authors in this publication
            Integer authorsCount = authors.size();

            // emit (year, no. authors, no. publications)
            return new Tuple3<String, Integer, Integer>(year, authorsCount, 1);
        }
    }

    // reducer: reduce by adding up no. authors & no. publications
    private static class AapReducer implements ReduceFunction<Tuple3<String, Integer, Integer>> {

        @Override
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
                                                       Tuple3<String, Integer, Integer> prev) throws Exception {

            // emit reduced tuple: (year, no. authors, no. publications)
            return new Tuple3<String, Integer, Integer>(current.f0, current.f1 + prev.f1, current.f2 + prev.f2);
        }
    }

    // mapper: maps (year, no. authors, no. publications) to (year, no. authors, no. publications, AAP)
    private static class AapAvgMapper implements MapFunction<Tuple3<String, Integer, Integer>,
            Tuple4<String, Integer, Integer, Double>> {

        @Override
        public Tuple4<String, Integer, Integer, Double> map(Tuple3<String, Integer, Integer> value) throws Exception {


            // return (year, no. authors, no. publications, avg no authors per paper AAP)
            return new Tuple4<String, Integer, Integer, Double>(
                    value.f0, // year
                    value.f1, // no. authors
                    value.f2, // no. publications
                    new Double((value.f1 * 1.0) / value.f2) // aap
            );
        }
    }
}