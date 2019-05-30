package batch;

enum PubVertexType {
    AUTHOR,
    PAPER,
    VENUE,
    PUBLISHER
}

public class PubVertexValue implements Comparable<PubVertexValue> {

    private Long label;
    private PubVertexType vertexType;

    public PubVertexValue(){}

    public PubVertexValue(Long label, PubVertexType vertexType){
        this.setLabel(label);
        this.setVertexType(vertexType);
    }

    public Long getLabel() {
        return label;
    }

    public void setLabel(Long label) {
        this.label = label;
    }

    public PubVertexType getVertexType() {
        return vertexType;
    }

    public void setVertexType(PubVertexType vertexType) {
        this.vertexType = vertexType;
    }

    @Override
    public int compareTo(PubVertexValue o) {
        return this.getLabel().compareTo(o.getLabel());
    }
}
