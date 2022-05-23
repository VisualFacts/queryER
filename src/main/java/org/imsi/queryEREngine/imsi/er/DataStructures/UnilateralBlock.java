package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;
import java.util.Arrays;

public class UnilateralBlock extends AbstractBlock implements Serializable {

	private static final long serialVersionUID = 43532585408538695L;

	protected final int[] entities;
	protected int[] queryEntities = new int[0];

	public UnilateralBlock(int[] entities) {
		super();
		this.entities = entities;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final UnilateralBlock other = (UnilateralBlock) obj;
		if (!Arrays.equals(this.entities, other.entities)) {
			return false;
		}
		return true;
	}

	public int[] getEntities() {
		return entities;
	}

	@Override
	public double getNoOfComparisons() {
		double noOfEntities = entities.length;
		double noOfQueryEntities = queryEntities.length;
		if (queryEntities.length > 0) {
			return noOfQueryEntities * noOfEntities;
		}
		return noOfEntities*(noOfEntities-1)/2;
	}

	@Override
	public double getTotalBlockAssignments() {
		return entities.length;
	}

	@Override
	public int hashCode() {
		int hash = 5;
		hash = 53 * hash + Arrays.hashCode(this.entities);
		return hash;
	}

	public void setQueryEntities(int[] queryEntities){
		this.queryEntities = queryEntities;
	}

	@Override
	public void setUtilityMeasure() {
		utilityMeasure = 1.0/entities.length;
	}
}