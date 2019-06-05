package stream;

import dblp.DblpParser;

import java.io.File;

public class ScipiKafkaStream {

    public static void main(String[] args) throws Exception {

        // get path where the DBLP data is saved
        String dblpUri = "/home/delinvas/Downloads/dblp.xml";
        File file = new File(dblpUri);
        DblpParser parser = new DblpParser(file);
    }
}
