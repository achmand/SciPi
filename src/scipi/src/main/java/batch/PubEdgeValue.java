package batch;

enum PubEdgeType {
    COAUTHORED, // > Author -> Co-Author (COAUTHORED)
    WRITTEN,    // > Author -> Paper (WRITTEN)
    PUBLISHED,  // > Paper  -> Publisher (PUBLISHED)
    PRESENTED,  // > Paper  -> Venue (PRESENTED)
    TAGGED,     // > Paper  -> Keyword (TAGGED)
    DOMAIN,     // > Paper  -> Field of study (DOMAIN)
}

public class PubEdgeValue {

    public PubEdgeType edgeType;

    public Integer weight;

    public PubEdgeValue(){}

    public PubEdgeValue(PubEdgeType edgeType, Integer weight){
        this.setEdgeType(edgeType);
        this.setWeight(weight);
    }

    public PubEdgeType getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(PubEdgeType edgeType) {
        this.edgeType = edgeType;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
