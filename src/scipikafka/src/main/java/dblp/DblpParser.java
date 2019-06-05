package dblp;

import java.io.*;
import java.util.Properties;
import javax.xml.parsers.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.xml.sax.*;
import org.xml.sax.helpers.*;

public class DblpParser {

    int line = 0;
    int errors = 0;
    StringBuffer author;
    // set properties for kafka
    Properties properties = new Properties();
    Producer<String, Paper> producer;
    private int curElement = -1;
    private int ancestor = -1;
    private Paper paper;
    private Conference conf;

    public DblpParser(File file) throws Exception {
        try {

            System.out.println("Parsing...");

            properties.setProperty("bootstrap.servers", "localhost:9092"); // IP address where Kafka is running
            properties.put("value.serializer", "stream.PaperSerializer");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, Paper>(properties);

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

                    producer.send(new ProducerRecord<String, Paper>("dblp", paper));

                    // TODO -> Push to kafka stream
                    // System.out.println(paper);
                }

                ancestor = Element.INPROCEEDING;
                curElement = Paper.INPROCEEDING;
                paper = new Paper();
                paper.key = atts.getValue("key");
            } else if (rawName.equals("proceedings")) {
                ancestor = Element.PROCEEDING;
                curElement = Conference.PROCEEDING;
                conf = new Conference();
                conf.key = atts.getValue("key");
            } else if (rawName.equals("author") && ancestor == Element.INPROCEEDING) {
                author = new StringBuffer();
            }

            if (ancestor == Element.INPROCEEDING) {
                curElement = Paper.getElement(rawName);
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
                if (curElement == Paper.AUTHOR) {
                    author.append(str);
                } else if (curElement == Paper.CITE) {
                    paper.citations.add(str);
                } else if (curElement == Paper.CONFERENCE) {
                    paper.conference = str;
                } else if (curElement == Paper.TITLE) {
                    paper.title += str;
                } else if (curElement == Paper.YEAR) {
                    paper.year = Integer.parseInt(str);
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
                paper.authors.add(author.toString().trim());
            }

            if (Element.getElement(rawName) == Element.INPROCEEDING) {
                ancestor = -1;

                if (paper.title.equals("") || paper.conference.equals("") || paper.year == 0) {
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