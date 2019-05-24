//package publication.mapper;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.util.Collector;
//import publication.OagPublication;
//
//public class YearWiseMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {
//
//    @Override
//    public void flatMap(OagPublication publication, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//        // get year
//        String year = publication.getYear();
//
//        // do not accept empty year, since year is defined as primary key in C* table
//        if (!year.isEmpty()) {
//            // emit (year : 1)
//            out.collect(new Tuple2<String, Integer>(year, 1));
//        }
//    }
//}
//
