package graphFrames;

public class Vertice {
	private String id;
	private String attribute;

	public Vertice(){      
	}

	public Vertice(String id, String attribute, int age) {
	    super();
	    this.id = id;
	    this.attribute = attribute;
	}

	public String getId() {
	    return id;
	}
	public void setId(String id) {
	    this.id = id;
	}
	public String getAttribute() {
	    return attribute;
	}
	public void setAttribute(String attribute) {
	    this.attribute = attribute;
	}
}