package publication;

/*
    Publications coming from Open Academic Graph.
    For more details visit: https://aminer.org/open-academic-graph
*/

import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.util.Set;
import java.util.UUID;

@Table(keyspace = "scipi", name = "publications")
public class Publication {

    @Column(name = "publication_doi")
    private String doi = "";

    @Column(name = "publication_title")
    private String title;

    @Column(name = "publication_id")
    private UUID id = UUIDs.timeBased();

    @Column(name = "publication_publisher")
    private String publisher;

    @Column(name = "publication_venue")
    private String venue;

    @Column(name = "publication_lang")
    private String lang;

    @Column(name = "publication_keywords")
    private Set<String> keywords;

    @Column(name = "publication_year")
    private String year;

    @Column(name = "publication_authors")
    private Set<String> authors;

    @Column(name="publication_fos")
    private Set<String> fos;

    @Column(name="publication_dataset")
    private String datasetType;

    public Publication() {
    }

    public Publication(String doi,
                          String title,
                          String publisher,
                          String venue,
                          String lang,
                          Set<String> keywords,
                          String year,
                          Set<String> authors,
                          Set<String> fos,
                          String datasetType
    ) {
        this.setDoi(doi);
        this.setTitle(title);
        this.setPublisher(publisher);
        this.setVenue(venue);
        this.setLang(lang);
        this.setKeywords(keywords);
        this.setYear(year);
        this.setAuthors(authors);
        this.setFos(fos);
        this.setDatasetType(datasetType);
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

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
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

    public Set<String> getFos() {
        return fos;
    }

    public void setFos(Set<String> fos) {
        this.fos = fos;
    }

    public String getDatasetType() {
        return datasetType;
    }

    public void setDatasetType(String datasetType) {
        this.datasetType = datasetType;
    }
}