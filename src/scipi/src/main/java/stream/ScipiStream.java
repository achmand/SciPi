package stream;

/*
    Handles/processes Kafka streams which contains publication using Apache Flink.
*/

// importing packages

import com.datastax.driver.mapping.Mapper;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.MapperOptions;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import publication.mapper.OagKwMapper;
import org.apache.flink.util.Collector;
import publication.OagPublication;
//import publication.mapper.YearWiseMapper;

import java.util.Properties;

/* Process Flow
 * 0: consume data stream from kafka
 * 1: map json strings passed from kafka to flink stream (POJO per publication)
 *      > only publications written in english
 *      > doi must not be empty as it is used as an id in CassandraDB
 *      > title must note be empty
 *      > at least a publisher or venue
 * 1.1: persist publications to CassandraDB using data sink
 * */

public class ScipiStream {

    // used to parse JSON to POJO
    private final static Gson gson = new Gson();

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

        // 0: consume data stream from kafka
        DataStream<String> kafkaData = environment.addSource(
                new FlinkKafkaConsumer<String>("oag", new SimpleStringSchema(), properties));

        // 1: first we map strings from Kafka to OagPublication using OagPubMapper
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

//        // 1. count occurrences for each keyword (used as topics at a later stage)
//        DataStream<Tuple2<String, Integer>> oagKeywords = oagPublications
//                .flatMap(new OagKwMapper())
//                .keyBy(0)
//                .sum(1);
//
//        // persist keyword count result into CassandraDB
//        CassandraSink.addSink(oagKeywords)
//                .setQuery("INSERT INTO scipi.oagkw(keyword, count) values (?, ?);")
//                .setHost("127.0.0.1")
//                .build();

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

    public static boolean isNullOrEmpty(String str) {

        if (str != null && !str.trim().isEmpty()) {
            return false;
        }

        return true;
    }

    // mapper: string to POJO (OagPublication)
    public static final class OagPubMapper implements FlatMapFunction<String, OagPublication> {

        @Override
        public void flatMap(String value, Collector<OagPublication> out) throws Exception {

            /* validate publication
                > only publication written in english (en)
                > no empty doi (used as id)
                > no empty title
                > must have at least a publisher or venue
            * */

            // parse string/json to OagPublication
            OagPublication publication = gson.fromJson(value, OagPublication.class);

            // validate language
            String lang = publication.getLang();

            // do not accept empty language
            if (lang == null || lang.trim().isEmpty()) {
                return;
            }

            // do not accept non english publications
//            if(lang.trim() != "en"){
//                return;
//            }

            // validate doi
            String doi = publication.getDoi();
            if (doi.isEmpty()) {
                return;
            }

            // set doi trimmed
            publication.setDoi(doi.trim());

            // title validation
            String title = publication.getTitle();

            // no empty title
            if (title.isEmpty()) {
                return;
            }

            // set title trimmed and to lower
            publication.setTitle(title
                    .toLowerCase()
                    .trim());

            // must have a publisher or a venue
//            String publisher = publication.getPublisher();
//            String venue = publication.getVenue();
//            if (publisher.isEmpty() && venue.isEmpty()) {
//                return;
//            }
//
//            // set publisher trimmed and lower if not empty
//            if (!publisher.isEmpty()) {
//                publication.setPublisher(publisher
//                        .toLowerCase()
//                        .trim());
//            }
//
//            // set venue trimmed and lower if not empty
//            if (!venue.isEmpty()) {
//                publication.setVenue(venue
//                        .toLowerCase()
//                        .trim());
//            }

            // collect publication
            out.collect(publication);
        }
    }
}