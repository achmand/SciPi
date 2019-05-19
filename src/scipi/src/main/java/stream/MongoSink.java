//package stream;
//
///*
//    MongoDB sink for Apace Flink stream.
//    Code taken from the following src:
//    https://github.com/lungothrin/yarn-flink-examples/blob/master/flink-streaming-mongodb-sink/src/main/java/example/Task.java
//
//    Some modifications were made: Adding constructor and passing mongoDb config.
//*/
//
//
//// importing packages
//import org.bson.Document;
//import org.bson.BSONObject;
//import com.mongodb.DBObject;
//import com.mongodb.BasicDBObject;
//import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.hadoop.io.BSONWritable;
//import com.mongodb.hadoop.io.MongoUpdateWritable;
//import com.mongodb.hadoop.MongoOutput;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//public class MongoSink<K, V> extends RichSinkFunction<Tuple2<K, V>> {
//    private String dbName;
//    private String collectionName;
//    private MongoCollection<Document> coll;
//
//    public MongoSink(String dbName, String collectionName) {
//        this.dbName = dbName;
//        this.collectionName = collectionName;
//    }
//
//    @Override
//    public void invoke(Tuple2<K, V> value, Context context) {
//        K key = value.getField(0);
//        V val = value.getField(1);
//
//        if (val instanceof MongoUpdateWritable) {
//            MongoUpdateWritable o = (MongoUpdateWritable) val;
//            coll.updateOne(new BasicDBObject(o.getQuery().toMap()),
//                    new BasicDBObject(o.getModifiers().toMap()));
//        } else {
//            DBObject o = new BasicDBObject();
//            if (key instanceof BSONWritable) {
//                o.put("_id", ((BSONWritable) key).getDoc());
//            } else if (key instanceof BSONObject) {
//                o.put("_id", key);
//            } else {
//                o.put("_id", BSONWritable.toBSON(key));
//            }
//
//            if (val instanceof BSONWritable) {
//                o.putAll(((BSONWritable) val).getDoc());
//            } else if (value instanceof MongoOutput) {
//                ((MongoOutput) val).appendAsValue(o);
//            } else if (value instanceof BSONObject) {
//                o.putAll((BSONObject) val);
//            } else {
//                o.put("value", BSONWritable.toBSON(val));
//            }
//
//            coll.insertOne(new Document(o.toMap()));
//        }
//    }
//
//    @Override
//    public void open(Configuration config) {
//        MongoClientURI uri = new MongoClientURI(
//                config.getString("mongo.output.uri",
//                        "mongodb://127.0.0.1:27017/" + dbName + "." + collectionName));
//
//        coll = new MongoClient(uri).getDatabase(uri.getDatabase())
//                .getCollection(uri.getCollection());
//    }
//}
