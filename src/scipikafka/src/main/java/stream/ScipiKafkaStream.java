package stream;

import dblp.DblpParser;

import java.io.Closeable;
import java.io.File;
import java.util.Map;


public class ScipiKafkaStream {

    public static int PRETTY_PRINT_INDENT_FACTOR = 4;

    public static void main(String[] args) throws Exception {

        // get path where the DBLP data is saved
        String dblpUri = "/home/delinvas/Downloads/dblp.xml";
        File file =  new File(dblpUri);
        DblpParser parser = new DblpParser(file);
    }
}
