package publication.mapper;

// importing packages
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import publication.OagPublication;

public class OagPubMapper implements MapFunction<String, OagPublication>{

    // used to parse JSON
    final static Gson gson = new Gson();

    @Override
    public OagPublication map(String value) throws Exception {
        return gson.fromJson(value, OagPublication.class);
    }
}