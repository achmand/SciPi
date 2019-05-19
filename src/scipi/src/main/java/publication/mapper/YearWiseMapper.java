package publication.mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import publication.OagPublication;

public class YearWiseMapper implements FlatMapFunction<OagPublication, Tuple2<Integer, Integer>> {

    @Override
    public void flatMap(OagPublication publication, Collector<Tuple2<Integer, Integer>> out) throws Exception {

        // get year
        String year = publication.getYear();

        // no year found
        if(year == null){
            return;
        }
         Integer result = Ints.tryParse(givenString);

        // emit (year : 1)
        out.collect(new Tuple2<Integer, Integer>(year, 1));
    }
}
