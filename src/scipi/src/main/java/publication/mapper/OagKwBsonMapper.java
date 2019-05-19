//package publication.mapper;
//
//// importing packages
//import com.mongodb.BasicDBObjectBuilder;
//import com.mongodb.hadoop.io.BSONWritable;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.bson.types.ObjectId;
//
//public class OagKwBsonMapper implements MapFunction<Tuple2<String, Integer>, Tuple2<ObjectId, BSONWritable>> {
//
//    @Override
//    public Tuple2<ObjectId, BSONWritable> map(Tuple2<String, Integer> tuple) throws Exception {
//        return new Tuple2<ObjectId, BSONWritable>(
//                new ObjectId(),
//                new BSONWritable(
//                        BasicDBObjectBuilder.start()
//                                .add("keyword", tuple.getField(0))
//                                .add("occurrence", tuple.getField(1))
//                                .get()
//                )
//        );
//    }
//}
