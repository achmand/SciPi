package publication;

/*
    Publications coming from Open Academic Graph.
    For more details visit: https://aminer.org/open-academic-graph
*/

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Set;

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

    @Column(name = "keywords")
    private Set<String> keywords;

    @Column(name = "year")
    private String year;

    @Column(name = "authors")
    private Set<String> authors;

    public OagPublication() {
    }

    public OagPublication(String doi,
                          String title,
                          String publisher,
                          String venue,
                          String lang,
                          Set<String> keywords,
                          String year,
                          Set<String> authors
    ) {
        this.setDoi(doi);
        this.setTitle(title);
        this.setPublisher(publisher);
        this.setVenue(venue);
        this.setLang(lang);
        this.setKeywords(keywords);
        this.setYear(year);
        this.setAuthors(authors);
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

    public Set<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Set<String> keywords) {
        this.keywords = keywords;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Set<String> getAuthors() {
        return authors;
    }

    public void setAuthors(Set<String> authors) {
        this.authors = authors;
    }
}