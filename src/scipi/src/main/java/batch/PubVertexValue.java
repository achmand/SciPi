package batch;

enum PubVertexType {
    NONE,
    AUTHOR,
    PAPER,
    VENUE,
    PUBLISHER
}

public class PubVertexValue {

    private Long vertexValue;
    private PubVertexType vertexType;

    public PubVertexValue(Long vertexValue, PubVertexType vertexType){
        this.setVertexValue(vertexValue);
        this.setVertexType(vertexType);
    }

    public Long getVertexValue() {
        return vertexValue;
    }

    public void setVertexValue(Long vertexName) {
        this.vertexValue = vertexName;
    }

    public PubVertexType getVertexType() {
        return vertexType;
    }

    public void setVertexType(PubVertexType vertexType) {
        this.vertexType = vertexType;
    }
}
