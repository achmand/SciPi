package publication.mapper;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import publication.OagPublication;

import java.util.ArrayList;

public class OagFosMapper implements FlatMapFunction<OagPublication, Tuple2<String, Integer>> {

    @Override
    public void flatMap(OagPublication publication, Collector<Tuple2<String, Integer>> out) throws Exception {

        // get all field of studies
        ArrayList<String> fields = publication.getFos();

        // if empty continue
        if (fields == null || fields.size() <= 0) {
            return;
        }

        // TODO -> Some issues with the filtering/constrains below...
        // map keyword to => (keyword : 1)
        for (String field : fields) {

            // keep only letters, numbers and spaces
            String cleanField = field.replaceAll("[^a-zA-Z0-9\\s]", "")
                    .toLowerCase()
                    .trim();

            // emit (fos : 1)
            out.collect(new Tuple2<String, Integer>(cleanField, 1));
        }
    }
}
