//package publication.mapper;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.util.Collector;
//import publication.OagPublication;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//import java.util.ArrayList;
//
//public class OagKwMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {
//
//    @Override
//    public void flatMap(OagPublication publication, Collector<Tuple2<String, Integer>> out) throws Exception {
//
//        // get all keywords
//        ArrayList<String> keywords = publication.getKeywords();
//
//        // if empty continue
//        if (keywords == null || keywords.size() <= 0) {
//            return;
//        }
//
//        // TODO -> Some issues with the filtering/constrains below...
//        // map keyword to => (keyword : 1)
//        for (String keyword : keywords) {
//
//            // keep only letters, numbers and spaces
//            String cleanKeyword = keyword.replaceAll("[^a-zA-Z0-9\\s]", "")
//                    .toLowerCase()
//                    .trim();
//
//            // if keyword is empty once cleaned continue
//            if (cleanKeyword.isEmpty()) {
//                continue;
//            }
//
//            // check that keyword does not exceed 30 char
//            if(cleanKeyword.length() > 30){
//                continue;
//            }
//
////            // if keyword contains only digits
////            if(cleanKeyword.replaceAll("\\s","")
////                    .matches("\\d+")){
////                continue;
////            }
//
//            // emit (keyword : 1)
//            out.collect(new Tuple2<String, Integer>(cleanKeyword, 1));
//        }
//    }
//}
//
