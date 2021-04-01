package org.imsi.queryEREngine.imsi.er.DataStructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import org.imsi.queryEREngine.imsi.er.Utilities.ComparisonIterator;

public abstract class AbstractBlock implements Serializable {

	private static final long serialVersionUID = 7526443743449L;

	protected int blockIndex;
	protected double utilityMeasure;

	public AbstractBlock() {
		blockIndex = -1;
		utilityMeasure = -1;
	}

	public int getBlockIndex() {
		return blockIndex;
	}

	public ComparisonIterator getComparisonIterator() {
		return new ComparisonIterator(this);
	}

	public double getUtilityMeasure() {
		return utilityMeasure;
	}

	public double processBlock(AbstractDuplicatePropagation adp) {
		double noOfComparisons = 0;

		ComparisonIterator iterator = getComparisonIterator();
		while (iterator.hasNext()) {
			Comparison comparison = iterator.next();
			if (!adp.isSuperfluous(comparison)) {
				noOfComparisons++;
			}
		}

		return noOfComparisons;
	}

	public void setBlockIndex(int blockIndex) {
		this.blockIndex = blockIndex;
	}

	public List<Comparison> getComparisons() {
		final List<Comparison> comparisons = new ArrayList<Comparison>();

		ComparisonIterator iterator = getComparisonIterator();
		while (iterator.hasNext()) {
			Comparison comparison = iterator.next();
			comparisons.add(comparison);
		}

		return comparisons;
	}

	public abstract double getNoOfComparisons();
	public abstract double getTotalBlockAssignments();
	public abstract void setUtilityMeasure();
}