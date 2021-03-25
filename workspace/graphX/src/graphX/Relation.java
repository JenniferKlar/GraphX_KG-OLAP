package graphX;

import java.io.Serializable;

public class Relation extends Object implements Serializable {

	private static final long serialVersionUID = 7086479564921920379L;
	private Resource relationship;
	private Resource context;
	private String targetDataType;
	
	
	public Relation(){
	}
	
	public Relation(Resource predicate , Resource context, String targetDataType) {
	    super();
	    this.relationship = predicate;
	    this.context = context;
	    this.targetDataType = targetDataType;
	    
	}
	
	public Resource getRelationship() {
	    return relationship;
	}
	
	public void setRelationship(Resource relationship) {
	    this.relationship = relationship;
	  }
	
	public Resource getContext() {
		return context;
	}
	
	public void setContext(Resource context) {
		this.context = context;
	}

	public String getTargetDataType() {
		return targetDataType;
	}

	public void setTargetDataType(String targetDataType) {
		this.targetDataType = targetDataType;
	}
	
}