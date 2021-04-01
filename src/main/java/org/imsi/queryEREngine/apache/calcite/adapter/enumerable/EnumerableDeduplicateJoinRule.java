package org.imsi.queryEREngine.apache.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.imsi.queryEREngine.apache.calcite.plan.Convention;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.convert.ConverterRule;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicateJoin;

public class EnumerableDeduplicateJoinRule extends ConverterRule {

	public EnumerableDeduplicateJoinRule() {
		super(LogicalDeduplicateJoin.class,
				(Predicate<RelNode>) r -> true,
				Convention.NONE, EnumerableConvention.INSTANCE,
				RelFactories.LOGICAL_BUILDER, "EnumerableDeduplicateJoinRule");
	}


	@Override
	public RelNode convert(RelNode rel) {
		final LogicalDeduplicateJoin deduplicateJoin = (LogicalDeduplicateJoin) rel;
		return EnumerableDeduplicateJoin.create(
				convert(deduplicateJoin.getLeft(),
					deduplicateJoin.getTraitSet()
						.replace(EnumerableConvention.INSTANCE)), convert(deduplicateJoin.getRight(),
								deduplicateJoin.getTraitSet()
								.replace(EnumerableConvention.INSTANCE)),
				deduplicateJoin.getCondition(), 
				deduplicateJoin.getVariablesSet(),
				deduplicateJoin.getJoinType(), deduplicateJoin.getSourceLeft(), deduplicateJoin.getSourceRight(),
				deduplicateJoin.getFieldTypesLeft(), deduplicateJoin.getFieldTypesRight(),
				deduplicateJoin.getKeyLeft(), deduplicateJoin.getKeyRight(),
				deduplicateJoin.getTableNameLeft(), deduplicateJoin.getTableNameRight(), deduplicateJoin.getFieldLeft(),
				deduplicateJoin.getFieldRight(),
				deduplicateJoin.isDirtyJoin());
	}
}
