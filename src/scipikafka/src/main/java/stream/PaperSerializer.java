package stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import dblp.Paper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PaperSerializer implements Serializer<Paper> {

    @Override public void configure(Map<String, ?> map, boolean b) {
    }

    @Override public byte[] serialize(String arg0, Paper arg1) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(arg1).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override public void close() {
    }
}