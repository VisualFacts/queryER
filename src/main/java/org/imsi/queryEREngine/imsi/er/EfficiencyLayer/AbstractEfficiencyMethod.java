package org.imsi.queryEREngine.imsi.er.EfficiencyLayer;

import java.util.List;

import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;



public abstract class AbstractEfficiencyMethod {

	private final String name;

	public AbstractEfficiencyMethod(String nm) {
		name = nm;
	}

	public String getName() {
		return name;
	}

	public abstract void applyProcessing(List<AbstractBlock> blocks);

	public void applyProcessing(List<AbstractBlock> blocks, AbstractDuplicatePropagation adp) {
		applyProcessing(blocks);

		double comparisons = 0;
		for (AbstractBlock block : blocks) {
			comparisons += block.processBlock(adp);
		}

		System.out.println("Detected duplicates\t:\t" + adp.getNoOfDuplicates());
		System.out.println("Executed comparisons\t:\t" + comparisons);
	}
}