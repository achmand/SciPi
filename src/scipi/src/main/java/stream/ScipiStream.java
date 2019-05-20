package stream;

/*
    Handles/processes Kafka streams which contains publication using Apache Flink.
*/

// importing packages
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import publication.mapper.OagKwMapper;
import publication.mapper.OagPubMapper;
import publication.OagPublication;
import publication.mapper.YearWiseMapper;

import java.util.Properties;

public class ScipiStream {

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

        // pull stream from Kafka to Flink's stream
        // must specify topic name, deserializer, properties
        DataStream<String> kafkaData = environment.addSource(
                new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        // first we map strings from Kafka to OagPublication using OagPubMapper
        // we use map instead flatMap since it is a strict one-to-one correspondence between in and out
        DataStream<OagPublication> oagPublications = kafkaData.map(new OagPubMapper());

        // 1. count occurrences for each keyword (used as topics at a later stage)
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

        // 3. year wise distribution of article publications
        DataStream<Tuple2<Integer, Integer>> yearWiseDist = oagPublications
                .flatMap(new YearWiseMapper())
                .keyBy(0)
                .sum(1);

        // persist year wise distribution result into CassandraDB
        CassandraSink.addSink(yearWiseDist)
                .setQuery("INSERT INTO scipi.yrwisedist(year, count) values (?, ?);")
                .setHost("127.0.0.1")
                .build();

        // group by keyword and sum value then consume result by mongo
//        oagKeywords.keyBy(0).sum(1).map(new OagKwBsonMapper()).
//                addSink(new MongoSink<ObjectId, BSONWritable>("scipi", "oagkw"));

        environment.execute("scipi stream processing");
    }
}