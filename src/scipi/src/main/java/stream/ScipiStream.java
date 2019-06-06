package stream;

/*
Handles/processes Kafka streams which contains publications using Apache Flink.

Notes
------
> Kafka consumer will periodically commit the offsets to Zookeeper, since check pointing is not enabled

Parameters
----------
> kafka_brokers: comma separated list for Kafka brokers (for local execution use localhost:9092)
> cassandra_point: the cassandra point (IP) that the driver uses to discover the cluster topology (local execution use 127.0.0.1)

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
import publication.Publication;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class ScipiStream {

    // kafka topics
    private final static String oagTopic = "oag"; // topic name for OAG data
    private final static String dblpTopic = "dblp"; // topic name for dblp data

    // used to parse JSON to POJO [topic:oag]
    private final static Gson oagGsonBuilder = new GsonBuilder()
            .registerTypeAdapter(Publication.class, new OagPubDeserializer())
            .create();

    // used to parse JSON to POJO [topic:dblp]
    private final static Gson dblpGsonBuilder = new GsonBuilder()
            .registerTypeAdapter(Publication.class, new DblpPubDeserializer())
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

        // gets kafka brokers IPs
        // comma separated list for Kafka brokers
        final String kafkaBrokers = parameters.get("kafka_brokers"); // for local execution use localhost:9092

        // set properties for kafka cluster
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);

        // NOTE: the Kafka consumer will periodically commit the offsets to Zookeeper,
        // since check pointing is not enabled

        // set up FlinkKafkaConsumer/s

        // kafka consumer for kafka stream (topic: oag)
        FlinkKafkaConsumer<String> kafkaOag = new FlinkKafkaConsumer<String>(
                oagTopic,
                new SimpleStringSchema(),
                properties);

        // start reading from partitions from the earliest record
        kafkaOag.setStartFromEarliest();

        // kafka consumer for kafka stream (topic: dblp)
        FlinkKafkaConsumer<String> kafkaDblp = new FlinkKafkaConsumer<String>(
                dblpTopic,
                new SimpleStringSchema(),
                properties);

        // start reading from partitions from the earliest record
        kafkaDblp.setStartFromEarliest();

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

        // consume data stream from kafka (oag topic)
        DataStream<String> kafkaOagStream = environment.addSource(kafkaOag);

        // consume data stream from kafka (dblp topic)
        DataStream<String> kafkaDblpStream = environment.addSource(kafkaDblp);

        // first we map json strings from Kafka oag topic to Publication using OagPubMapper
        DataStream<Publication> oagPublicationsStream = kafkaOagStream.flatMap(new OagPubMapper());

        // then we map json strings from Kafka dblp topic to Publication using DblpPubMapper
        DataStream<Publication> dblpPublicationStream = kafkaDblpStream.flatMap(new DblpPubMapper());

        // combine both streams coming from different sources
        DataStream<Publication> publicationStream = oagPublicationsStream.union(dblpPublicationStream);

        // persist OagPublications to CassandraDB using data sink
        // add sink for 'publications', see OagPublication class for C* columns/table definitions
        CassandraSink.addSink(publicationStream)
                .setClusterBuilder(cassandraBuilder)
                .setMapperOptions(new MapperOptions() {
                    @Override
                    public Mapper.Option[] getMapperOptions() {
                        return new Mapper.Option[]{Mapper.Option.saveNullFields(true)};
                    }
                }).build();

        DataStream<Tuple2<String, Long>> keywordStream = publicationStream
                .flatMap(new KeywordMapper()) // map
                .keyBy(0)           // key by keyword
                .sum(1);       // sum the emitted 1

        CassandraSink.addSink(keywordStream)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.keywords(keyword_name, keyword_count) values (?, ?);")
                .build();

        DataStream<Tuple2<String, Long>> fieldOfStudyStream = publicationStream
                .flatMap(new FieldOfStudyMapper())    // map
                .keyBy(0)               // key by field of study
                .sum(1);           // sum the emitted 1

        CassandraSink.addSink(fieldOfStudyStream)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.field_study(field_study_name, field_study_count) values (?, ?);")
                .build();

        DataStream<Tuple6<String, Long, Long, Long, Double, Double>> yrWiseDist = publicationStream
                .map(new YearWiseMapper())       // map
                .keyBy(0)                // key by year published
                .reduce(new YearWiseReducer())   // reduce by counting single & co-authored
                .map(new YearPercMapper());      // set percentages

        CassandraSink.addSink(yrWiseDist)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.yrwisedist(year, single, joint, total, single_perc, joint_perc)" +
                        " values (?, ?, ?, ?, ?, ?);")
                .build();

        DataStream<Tuple3<Integer, Long, Long>> authorshipPattern = publicationStream
                .map(new AuthorshipMapper())      // map  (no. authors, no. articles, tot no. authors)
                .keyBy(0)                 // key by no. authors (unit)
                .reduce(new AuthorshipReducer()); // reduce by adding up total publications and total authors

        CassandraSink.addSink(authorshipPattern)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.authorptrn(author_unit, no_articles, no_authors) values (?, ?, ?);")
                .build();

        DataStream<Tuple4<String, Long, Long, Double>> aap = publicationStream
                .map(new AapMapper())
                .keyBy(0)
                .reduce(new AapReducer())
                .map(new AapAvgMapper());

        CassandraSink.addSink(aap)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.aap(year, no_authors, no_articles, avg_author_paper) values (?, ?, ?, ?);")
                .build();

        DataStream<Tuple2<String, Long>> hyperAuthorship = publicationStream
                .flatMap(new HyperAuthorshipMapper()) // map (year, 1)
                .keyBy(0)                     // key by year
                .sum(1);                 // sum occurrences

        CassandraSink.addSink(hyperAuthorship)
                .setClusterBuilder(cassandraBuilder)
                .setQuery("INSERT INTO scipi.hyper_authorship(hyper_authorship_year, hyper_authorship_count) values (?, ?);")
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

    // Publication JSON deserializer [topic:oag]
    private static class OagPubDeserializer implements JsonDeserializer<Publication> {

        @Override
        public Publication deserialize(JsonElement jsonElement,
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
            return new Publication(
                    doi,
                    title,
                    publisher,
                    venue,
                    lang,
                    keywords,
                    year,
                    authors,
                    fos,
                    oagTopic);
        }
    }

    // Publication JSON deserializer [topic:dblp]
    private static class DblpPubDeserializer implements JsonDeserializer<Publication> {

        @Override
        public Publication deserialize(JsonElement jsonElement,
                                       Type type,
                                       JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

            // get json object
            JsonObject jsonObject = jsonElement.getAsJsonObject();

            // for publications coming from DBLP we will add computer science as keyword
            HashSet<String> keywords = new HashSet<String>(1);
            keywords.add("computer science");

            // for publications coming from DBLP we will add computer science as field of study
            HashSet<String> fos = new HashSet<String>(1);
            fos.add("computer science");

            // get authors
            HashSet<String> authors = null;

            // check that json array is not empty
            if (jsonObject.get("authors") != null) {
                JsonArray jsonAuthors = jsonObject.get("authors").getAsJsonArray();
                if (jsonAuthors.size() > 0) {
                    authors = new HashSet<String>(jsonAuthors.size());
                    for (JsonElement author : jsonAuthors) {
                        String authorName = author.getAsString();
                        if (!authors.contains(authorName)) {
                            authors.add(authorName);
                        }
                    }
                }
            }

            String key = null;
            if (jsonObject.get("key") != null) {
                key = jsonObject.get("key").getAsString();
            }

            String title = null;
            if (jsonObject.get("title") != null) {
                title = jsonObject.get("title").getAsString();
            }

            String venue = null;
            if (jsonObject.get("conference") != null) {
                venue = jsonObject.get("conference").getAsString();
            }

            String year = null;
            if (jsonObject.get("year") != null) {
                year = jsonObject.get("year").getAsString();
            }

            // return publication
            return new Publication(
                    key,
                    title,
                    null,
                    venue,
                    "en",
                    keywords,
                    year,
                    authors,
                    fos,
                    dblpTopic);
        }
    }

    // mapper: string to POJO (Publication) for OAG
    private static final class OagPubMapper implements FlatMapFunction<String, Publication> {

        @Override
        public void flatMap(String value, Collector<Publication> out) throws Exception {

            // parse string/json to Publication
            Publication publication = oagGsonBuilder.fromJson(value, Publication.class);

            // validate language
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

    // mapper: string to POJO (Publication) for DBLP
    private static final class DblpPubMapper implements FlatMapFunction<String, Publication> {

        @Override
        public void flatMap(String value, Collector<Publication> out) throws Exception {

            // parse string/json to Publication
            Publication publication = dblpGsonBuilder.fromJson(value, Publication.class);

            // validate language
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

    // mapper: Publication to Tuple<String, Long> to count occurrences (keywords)
    private static final class KeywordMapper implements FlatMapFunction<Publication, Tuple2<String, Long>> {

        @Override
        public void flatMap(Publication value, Collector<Tuple2<String, Long>> out) throws Exception {

            // get keyword set from OagPublication
            Set<String> keywords = value.getKeywords();

            // check if keywords set is valid
            if (keywords != null && keywords.size() > 0) {

                // no need to validate since OagPublication was validated at an early stage
                // map keyword to => (keyword : 1)
                for (String keyword : keywords) {

                    // emit (keyword : 1)
                    out.collect(new Tuple2<String, Long>(keyword, 1L));
                }
            }
        }
    }

    // mapper: Publication to Tuple<String, Long> to count occurrences (field of study)
    private static final class FieldOfStudyMapper implements FlatMapFunction<Publication, Tuple2<String, Long>> {

        @Override
        public void flatMap(Publication value, Collector<Tuple2<String, Long>> out) throws Exception {

            // get fos set from OagPublication
            Set<String> fos = value.getFos();

            // check if fos set is valid)
            if (fos != null && fos.size() > 0) {

                // no need to validate since OagPublication was validated at an early stage
                // map keyword to => (field : 1)
                for (String field : fos) {

                    // emit (fos : 1)
                    out.collect(new Tuple2<String, Long>(field, 1L));
                }
            }
        }
    }

    // mapper: Publication to Tuple<String, int> to count occurrences (single vs co-authored publications)
    private static class YearWiseMapper implements MapFunction<Publication, Tuple3<String, Long, Long>> {

        @Override
        public Tuple3<String, Long, Long> map(Publication publication) throws Exception {

            // get year from OagPublication
            String year = publication.getYear();

            // get authors from OagPublication
            Set<String> authors = publication.getAuthors();

            // set single and joint authored
            Integer authorsCount = authors.size();
            Long single = authorsCount == 1 ? 1L : 0;
            Long joint = authorsCount > 1 ? 1L : 0;

            // emit (year, single, joint)
            return new Tuple3<String, Long, Long>(year, single, joint);
        }
    }

    // reducer: reduce by adding up single-author and co-authored publications
    private static class YearWiseReducer implements ReduceFunction<Tuple3<String, Long, Long>> {

        @Override
        public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> current,
                                                 Tuple3<String, Long, Long> pre) throws Exception {

            // emit reduced tuple (year, single, joint)
            return new Tuple3<String, Long, Long>(current.f0, current.f1 + pre.f1, current.f2 + pre.f2);
        }
    }

    // mapper: maps (yr, single, co-authored) to (yr, single, co-authored, %single, %co-authored)
    private static class YearPercMapper implements MapFunction<Tuple3<String, Long, Long>,
            Tuple6<String, Long, Long, Long, Double, Double>> {

        @Override
        public Tuple6<String, Long, Long, Long, Double, Double> map(
                Tuple3<String, Long, Long> value) throws Exception {

            // sum total single and co-authored to get total publications
            Long totalPub = value.f1 + value.f2;

            // return (yr, tot single, tot co-authored, tot pub, %single, %co-authored)
            return new Tuple6<String, Long, Long, Long, Double, Double>(
                    value.f0, // year
                    value.f1, // single author
                    value.f2, // co-authored
                    totalPub, // total publications
                    new Double((value.f1 * 1.0) / totalPub), // percentage single author
                    new Double((value.f2 * 1.0) / totalPub)  // percentage co-authored
            );
        }
    }

    // mapper: maps Publication to (no. authors, no. articles, tot no. authors)
    private static class AuthorshipMapper implements MapFunction<Publication,
            Tuple3<Integer, Long, Long>> {

        @Override
        public Tuple3<Integer, Long, Long> map(Publication publication) throws Exception {

            // no of authors who worked on this publication (unit)
            Integer noAuthors = publication.getAuthors().size();

            // return (no. authors, no. articles, tot no.authors)
            return new Tuple3<Integer, Long, Long>(noAuthors, 1L, noAuthors.longValue());
        }
    }

    // reducer: reduce by adding up total publications and total authors
    private static class AuthorshipReducer implements ReduceFunction<Tuple3<Integer, Long, Long>> {

        @Override
        public Tuple3<Integer, Long, Long> reduce(Tuple3<Integer, Long, Long> current,
                                                  Tuple3<Integer, Long, Long> prev) throws Exception {

            // sum up total publications
            Long totPublications = current.f1 + prev.f1;

            // total publications * no.authors (unit)
            Long totAuthors = totPublications * current.f0;

            // emit reduced tuple (no. authors, no. articles, tot no.authors)
            return new Tuple3<Integer, Long, Long>(current.f0, totPublications, totAuthors);
        }
    }

    // mapper: maps Publication to (year, no. authors, no. publications)
    private static class AapMapper implements MapFunction<Publication, Tuple3<String, Long, Long>> {

        @Override
        public Tuple3<String, Long, Long> map(Publication publication) throws Exception {

            // get year from OagPublication
            String year = publication.getYear();

            // get authors from OagPublication
            Set<String> authors = publication.getAuthors();

            // get total number of authors in this publication
            Integer authorsCount = authors.size();

            // emit (year, no. authors, no. publications)
            return new Tuple3<String, Long, Long>(year, authorsCount.longValue(), 1L);
        }
    }

    // reducer: reduce by adding up no. authors & no. publications
    private static class AapReducer implements ReduceFunction<Tuple3<String, Long, Long>> {

        @Override
        public Tuple3<String, Long, Long> reduce(Tuple3<String, Long, Long> current,
                                                 Tuple3<String, Long, Long> prev) throws Exception {

            // emit reduced tuple: (year, no. authors, no. publications)
            return new Tuple3<String, Long, Long>(current.f0, current.f1 + prev.f1, current.f2 + prev.f2);
        }
    }

    // mapper: maps (year, no. authors, no. publications) to (year, no. authors, no. publications, AAP)
    private static class AapAvgMapper implements MapFunction<Tuple3<String, Long, Long>,
            Tuple4<String, Long, Long, Double>> {

        @Override
        public Tuple4<String, Long, Long, Double> map(Tuple3<String, Long, Long> value) throws Exception {


            // return (year, no. authors, no. publications, avg no authors per paper AAP)
            return new Tuple4<String, Long, Long, Double>(
                    value.f0, // year
                    value.f1, // no. authors
                    value.f2, // no. publications
                    new Double((value.f1 * 1.0) / value.f2) // aap
            );
        }
    }

    private static class HyperAuthorshipMapper implements FlatMapFunction<Publication, Tuple2<String, Long>> {

        @Override
        public void flatMap(Publication publication,
                            Collector<Tuple2<String, Long>> out) throws Exception {

            // get authors
            Set<String> authors = publication.getAuthors();
            if (authors.size() < 100) {
                return;
            }

            // get year
            String year = publication.getYear();

            // publication has more than 500 authors
            out.collect(new Tuple2<String, Long>(year, 1L));
        }
    }
}