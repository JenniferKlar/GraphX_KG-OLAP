package graphX;

import java.io.Serializable;

public class Relation extends Object implements Serializable {

	private static final long serialVersionUID = 7086479564921920379L;
	private Object relationship;
	private Object context;
	private String targetDataType;

	public Relation() {
	}

	public Relation(Object predicate, Object context, String targetDataType) {
		super();
		this.relationship = predicate;
		this.context = context;
		this.targetDataType = targetDataType;

	}

	public Object getRelationship() {
		return relationship;
	}

	public void setRelationship(Object relationship) {
		this.relationship = relationship;
	}

	public String getContext() {
		return context.toString();
	}

	public void setContext(Object context) {
		this.context = context;
	}
	public Relation updateContext(Object newContext) {
		return new Relation(this.relationship, newContext, this.targetDataType);
	}

	public String getTargetDataType() {
		return targetDataType;
	}

	public void setTargetDataType(String targetDataType) {
		this.targetDataType = targetDataType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((context == null) ? 0 : context.hashCode());
		result = prime * result + ((relationship == null) ? 0 : relationship.hashCode());
		result = prime * result + ((targetDataType == null) ? 0 : targetDataType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Relation other = (Relation) obj;
		if (context == null) {
			if (other.context != null)
				return false;
		} else if (!context.equals(other.context))
			return false;
		if (relationship == null) {
			if (other.relationship != null)
				return false;
		} else if (!relationship.equals(other.relationship))
			return false;
		if (targetDataType == null) {
			if (other.targetDataType != null)
				return false;
		} else if (!targetDataType.equals(other.targetDataType))
			return false;
		return true;
	}

}