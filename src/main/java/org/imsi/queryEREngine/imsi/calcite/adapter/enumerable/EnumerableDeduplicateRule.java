package org.imsi.queryEREngine.imsi.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.imsi.queryEREngine.apache.calcite.plan.Convention;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.convert.ConverterRule;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicate;

/**
 * 
 * @author bstam
 * The rule that converts a LogicalDeduplicate to an EnumerableDeduplicate for the 
 * physical plan implementation.
 */
public class EnumerableDeduplicateRule extends ConverterRule {

	public EnumerableDeduplicateRule() {
		super(LogicalDeduplicate.class,
				(Predicate<RelNode>) r -> true,
				Convention.NONE, EnumerableConvention.INSTANCE,
				RelFactories.LOGICAL_BUILDER, "EnumerableDeduplicateRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		LogicalDeduplicate deduplicate = (LogicalDeduplicate)rel;
		RelNode input = deduplicate.getInput(0);
		return EnumerableDeduplicate.create(
				convert(input, input
						.getTraitSet()
						.replace(EnumerableConvention.INSTANCE)), deduplicate
				.getRelTable(), deduplicate.getBlockIndex(), deduplicate
				.getConjuctions(), deduplicate.getKey(), deduplicate.getSource(), deduplicate
				.getFieldTypes(), deduplicate.getComparisons());
	}
}
