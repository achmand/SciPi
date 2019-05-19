package publication;

/*
    Publications coming from Open Academic Graph.
    For more details visit: https://aminer.org/open-academic-graph
*/

import java.util.ArrayList;

public class OagPublication {

    private String title;
    private ArrayList<OagAuthor> authors;
    private String venue;
    private String year;
    private ArrayList<String> keywords;
    private ArrayList<String> fos;
    private String publisher;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public ArrayList<OagAuthor> getAuthors() {
        return authors;
    }

    public void setAuthors(ArrayList<OagAuthor> authors) {
        this.authors = authors;
    }

    public String getVenue() {
        return venue;
    }

    public void setVenue(String venue) {
        this.venue = venue;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public ArrayList<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(ArrayList<String> keywords) {
        this.keywords = keywords;
    }

    public ArrayList<String> getFos() {
        return fos;
    }

    public void setFos(ArrayList<String> fos) {
        this.fos = fos;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }
}
