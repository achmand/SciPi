package dblp;

import java.io.*;
import java.util.Properties;
import javax.xml.parsers.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

public class DblpParser {

    private static ObjectMapper mapper = new ObjectMapper();
    int line = 0;
    int errors = 0;
    StringBuffer author;

    // set properties for kafka
    Properties properties = new Properties();
    Producer<String, String> producer;
    private int curElement = -1;
    private int ancestor = -1;
    private DblpPublication paper;
    private Conference conf;

    public DblpParser(File file) throws Exception {
        try {

            System.out.println("Parsing...");

            properties.setProperty("bootstrap.servers", "35.158.135.251:9092,18.197.44.232:9092,18.185.136.226:9092"); // IP address where Kafka is running
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>(properties);

            SAXParserFactory parserFactory = SAXParserFactory.newInstance();
            SAXParser parser = parserFactory.newSAXParser();
            ConfigHandler handler = new ConfigHandler();
            parser.getXMLReader().setFeature("http://xml.org/sax/features/validation", true);
            parser.parse(file, handler);

            try {
                System.out.println("Processed " + line);
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("num of errors: " + errors);
        } catch (IOException e) {
            System.out.println("Error reading URI: " + e.getMessage());
        } catch (SAXException e) {
            System.out.println("Error in parsing: " + e.getMessage());
        } catch (ParserConfigurationException e) {
            System.out.println("Error in XML parser configuration: "
                    + e.getMessage());
        }
    }

    private class ConfigHandler extends DefaultHandler {

        public void startElement(String namespaceURI, String localName,
                                 String rawName, Attributes atts) throws SAXException {

            if (rawName.equals("inproceedings")) {

                if (paper != null) {

                    try {
                        // convert publication to json and push to kafka broker
                        String jsonPaper = mapper.writeValueAsString(paper);
                        producer.send(new ProducerRecord<String, String>("dblp", jsonPaper));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }

                ancestor = Element.INPROCEEDING;
                curElement = DblpPublication.INPROCEEDING;

                paper = new DblpPublication();
                paper.setKey(atts.getValue("key"));
            } else if (rawName.equals("proceedings")) {
                ancestor = Element.PROCEEDING;
                curElement = Conference.PROCEEDING;
                conf = new Conference();
                conf.key = atts.getValue("key");
            } else if (rawName.equals("author") && ancestor == Element.INPROCEEDING) {
                author = new StringBuffer();
            }

            if (ancestor == Element.INPROCEEDING) {
                curElement = DblpPublication.getElement(rawName);
            } else if (ancestor == Element.PROCEEDING) {
                curElement = Conference.getElement(rawName);
            } else if (ancestor == -1) {
                ancestor = Element.OTHER;
                curElement = Element.OTHER;
            } else {
                curElement = Element.OTHER;
            }

            line++;
        }

        public void characters(char[] ch, int start, int length) throws SAXException {
            if (ancestor == Element.INPROCEEDING) {
                String str = new String(ch, start, length).trim();
                if (curElement == DblpPublication.AUTHOR) {
                    author.append(str);
                } else if (curElement == DblpPublication.CITE) {
                    paper.getCitations().add(str);
                } else if (curElement == DblpPublication.CONFERENCE) {
                    paper.setConference(str);
                } else if (curElement == DblpPublication.TITLE) {
                    String tmpTitle = paper.getTitle();
                    paper.setTitle(tmpTitle += str);
                } else if (curElement == DblpPublication.YEAR) {
                    paper.setYear(Integer.parseInt(str));
                }
            } else if (ancestor == Element.PROCEEDING) {
                String str = new String(ch, start, length).trim();
                if (curElement == Conference.CONFNAME) {
                    conf.name = str;
                } else if (curElement == Conference.CONFDETAIL) {
                    conf.detail = str;
                }
            }
        }

        public void endElement(String namespaceURI, String localName, String rawName) throws SAXException {

            if (rawName.equals("author") && ancestor == Element.INPROCEEDING) {
                paper.getAuthors().add(author.toString().trim());
            }

            if (Element.getElement(rawName) == Element.INPROCEEDING) {
                ancestor = -1;

                if (paper.getTitle().equals("") || paper.getConference().equals("") || paper.getYear() == 0) {
                    System.out.println("Error in parsing " + paper);
                    errors++;
                    return;
                }

            } else if (Element.getElement(rawName) == Element.PROCEEDING) {
                ancestor = -1;

                if (conf.name.equals("")) {
                    conf.name = conf.detail;
                }
                if (conf.key.equals("") || conf.name.equals("") || conf.detail.equals("")) {
                    System.out.println("Line:" + line);
                    System.exit(0);
                }
            }

        }

        private void Message(String mode, SAXParseException exception) {
            System.out.println(mode + " Line: " + exception.getLineNumber()
                    + " URI: " + exception.getSystemId() + "\n" + " Message: "
                    + exception.getMessage());
        }

        public void warning(SAXParseException exception) throws SAXException {
            Message("**Parsing Warning**\n", exception);
            throw new SAXException("Warning encountered");
        }

        public void error(SAXParseException exception) throws SAXException {
            Message("**Parsing Error**\n", exception);
            throw new SAXException("Error encountered");
        }

        public void fatalError(SAXParseException exception) throws SAXException {
            Message("**Parsing Fatal Error**\n", exception);
            throw new SAXException("Fatal Error encountered");
        }
    }
}