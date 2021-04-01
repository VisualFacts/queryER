package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;


public class Attribute implements Serializable {

	private static final long serialVersionUID = 1245324342344634589L;

	private final Integer index;
	private final String value;

	public Attribute (Integer ind, String val) {
		index = ind;
		value = val;
	}

	
	public Integer getIndex() {
		return index;
	}

	public String getValue() {
		return value;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((index == null) ? 0 : index.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		Attribute other = (Attribute) obj;
		if (index == null) {
			if (other.index != null)
				return false;
		} else if (!index.equals(other.index))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	
}