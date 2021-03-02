package graphFrames;

public class Relation {

private long src;
private long dst;
private String relationship;
private String context;


public Relation(){

}

public Relation(long src, long dst, String relationship) {
    super();
    this.src = src;
    this.dst = dst;
    this.relationship = relationship;
    this.context = context;
}

public long getSrc() {
    return src;
}

public void setSrc(long src) {
    this.src = src;
}

public long getDst() {
    return dst;
}

public void setDst(long dst) {
    this.dst = dst;
}

public String getRelationship() {
    return relationship;
}

public void setRelationship(String relationship) {
    this.relationship = relationship;
  }

public String getContext() {
	return context;
}

public void setContext(String context) {
	this.context = context;
}

}