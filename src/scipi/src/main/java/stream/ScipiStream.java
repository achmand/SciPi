package stream;

/*
    Handles/processes Kafka streams which contains publication using Apache Flink.
*/

// importing packages

import com.datastax.driver.mapping.Mapper;
import com.google.gson.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import publication.OagPublication;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/* Process Flow
----------------
 * 0.0: consume data stream from kafka
 *
 * 1.0: map json strings passed from kafka to flink stream (POJO per publication)
 *      > 1.0.1: only publications written in english
 *      > 1.0.2: doi must not be empty as it is used as an id in CassandraDB
 *      > 1.0.3: title must not be empty
 *      > 1.0.4: at least a publisher or venue
 *      > 1.0.5: at least one keyword or field of study
 *          > 1.0.5.1: clean keywords and keep only valid ones
            > 1.0.5.2: clean fos and keep only valid ones
 *      > 1.0.6: must have a valid year
 *      > 1.0.7: must have at least one author
            > 1.0.7.1: clean authors and keep onl valid ones
 *
 * 1.1: persist publications to CassandraDB using data sink
 *
 * 2.0: map OagPublication to Tuple<str,int> and count keyword occurrences
 *
 * 2.1: persist occurrences count for keyword to CassandraDB using data sink
 * */

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

        // 0.0: consume data stream from kafka
        DataStream<String> kafkaData = environment.addSource(
                new FlinkKafkaConsumer<String>("oag", new SimpleStringSchema(), properties));

        // 1.0: first we map strings from Kafka to OagPublication using OagPubMapper
        DataStream<OagPublication> oagPublications = kafkaData.flatMap(new OagPubMapper());

        // 1.1: persist publications to CassandraDB using data sink
        CassandraSink.addSink(oagPublications)
                .setHost("127.0.0.1")
                .setMapperOptions(new MapperOptions() {
                    @Override
                    public Mapper.Option[] getMapperOptions() {
                        return new Mapper.Option[]{Mapper.Option.saveNullFields(true)};
                    }
                }).build();

//        // 2.0: map OagPublication to Tuple<str,int> and count keyword occurrences
//        DataStream<Tuple2<String, Integer>> oagKeywords = oagPublications
//                .flatMap(new OagKwMapper())
//                .keyBy(0)        // key by keyword
//                .sum(1);    // sum the emitted 1

        // 2.1: persist occurrences count for keyword to CassandraDB using data sink
//        CassandraSink.addSink(oagKeywords)
//                .setQuery("INSERT INTO scipi.oagkw(keyword, count) values (?, ?);")
//                .setHost("127.0.0.1")
//                .build();

//        // 1. count occurrences for each keyword (used as topics at a later stage)
//        DataStream<Tuple2<String, Integer>> oagKeywords = oagPublications
//                .flatMap(new OagKwMapper())
//                .keyBy(0)
//                .sum(1);

//        // 2. count occurrences for each field of study (used as domains at a later stage)
//        DataStream<Tuple2<String, Integer>> oagFields = oagPublications
//                .flatMap(new OagFosMapper())
//                .keyBy(0)
//                .sum(1);
//
//        // persist field count result into CassandraDB
//        CassandraSink.addSink(oagFields)
//                .setQuery("INSERT INTO scipi.oagfos(fos, count) values (?, ?);")
//                .setHost("127.0.0.1")
//                .build();

//        // 3. year wise distribution of article publications
//        DataStream<Tuple2<String, Integer>> yearWiseDist = oagPublications
//                .flatMap(new YearWiseMapper())
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .sum(1);
//
//        // persist year wise distribution result into CassandraDB
//        CassandraSink.addSink(yearWiseDist)
//                .setQuery("INSERT INTO scipi.yrwisedist(year, count) values (?, ?);")
//                .setHost("127.0.0.1")
//                .build();

        // group by keyword and sum value then consume result by mongo
//        oagKeywords.keyBy(0).sum(1).map(new OagKwBsonMapper()).
//                addSink(new MongoSink<ObjectId, BSONWritable>("scipi", "oagkw"));

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

        str = str.trim();
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
            topic = topic.replaceAll("[^a-zA-Z0-9\\s]", "");
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
    public static final class OagPubMapper implements FlatMapFunction<String, OagPublication> {

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

    // mapper: OagPublication to Tuple<String, int> to count occurrences
    public static final class OagKwMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {

        @Override
        public void flatMap(OagPublication value, Collector<Tuple2<String, Integer>> out) throws Exception {

            // get keyword set from OagPublication
            Set<String> keywords = value.getKeywords();

            // no need to validate since OagPublication was validated at an early stage
            // map keyword to => (keyword : 1)
            for (String keyword : keywords) {

                // emit (keyword : 1)
                out.collect(new Tuple2<String, Integer>(keyword, 1));
            }
        }
    }
}