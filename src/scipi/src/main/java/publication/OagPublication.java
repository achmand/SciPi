package publication;

/*
    Publications coming from Open Academic Graph.
    For more details visit: https://aminer.org/open-academic-graph
*/

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.util.ArrayList;

// having issues persisting POJO to cassandra directly
// stored procedures are use instead

@Table(keyspace = "scipi", name = "oagpub")
public class OagPublication {

    @Column(name = "doi")
    private String doi = "";

    @Column(name = "title")
    private String title;

    @Column(name = "publisher")
    private String publisher;

    @Column(name = "venue")
    private String venue;

    @Column(name = "lang")
    private String lang;

//    private ArrayList<OagAuthor> authors;
//    private String venue;
//    private String year;
//    private ArrayList<String> keywords;
//    private ArrayList<String> fos;
//    private String publisher;

    public OagPublication() {
    }

    public OagPublication(String doi, String title, String publisher, String venue, String lang) {
        this.setDoi(doi);
        this.setTitle(title);
        this.setPublisher(publisher);
        this.setVenue(venue);
        this.setLang(lang);
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getVenue() {
        return venue;
    }

    public void setVenue(String venue) {
        this.venue = venue;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

//    public ArrayList<OagAuthor> getAuthors() {
//        return authors;
//    }
//
//    public void setAuthors(ArrayList<OagAuthor> authors) {
//        this.authors = authors;
//    }
//
//    public String getVenue() {
//        return venue;
//    }
//
//    public void setVenue(String venue) {
//        this.venue = venue;
//    }
//
//    public String getYear() {
//        return year;
//    }
//
//    public void setYear(String year) {
//        this.year = year;
//    }
//
//    public ArrayList<String> getKeywords() {
//        return keywords;
//    }
//
//    public void setKeywords(ArrayList<String> keywords) {
//        this.keywords = keywords;
//    }
//
//    public ArrayList<String> getFos() {
//        return fos;
//    }
//
//    public void setFos(ArrayList<String> fos) {
//        this.fos = fos;
//    }
//
//    public String getPublisher() {
//        return publisher;
//    }
//
//    public void setPublisher(String publisher) {
//        this.publisher = publisher;
//    }
}