package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;

public class IdDuplicates implements Serializable {

	private static final long serialVersionUID = 7234234586147L;

	private final int entityId1;
	private final int entityId2;

	public IdDuplicates(int l, int m) {
		entityId1 = l;
		entityId2 = m;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result + entityId1);
		result = (prime * result + entityId2);
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
		IdDuplicates other = (IdDuplicates) obj;
		if (entityId1 != other.entityId1)
			return false;
		if (entityId2 != other.entityId2)
			return false;
		return true;
	}


	public int getEntityId1() {
		return entityId1;
	}

	public int getEntityId2() {
		return entityId2;
	}


	@Override
	public String toString() {
		return "IdDuplicates [entityId1=" + entityId1 + ", entityId2=" + entityId2 + "]";
	}
	
	
}