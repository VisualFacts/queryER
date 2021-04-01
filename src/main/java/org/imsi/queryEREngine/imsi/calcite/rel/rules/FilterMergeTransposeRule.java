package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.Filter;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalFilter;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilder;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.core.MergeEntities;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalMergeEntities;

/**
 * 
 * @author bstam
 * Rule for transposing a filter and a mergeEntities rel opt. 
 * This is to move MergeEntities above the Deduplicate rel opt where it originally resides.
 */
public class FilterMergeTransposeRule extends RelOptRule {

	public static final FilterMergeTransposeRule INSTANCE =
			new FilterMergeTransposeRule(LogicalFilter.class, LogicalMergeEntities.class,
					RelFactories.LOGICAL_BUILDER);


	public FilterMergeTransposeRule(
			Class<? extends Filter> filterClass,
			Class<? extends MergeEntities> mergeEntitiesClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						filterClass,
						operand(mergeEntitiesClass, any())),
				relBuilderFactory);
	}

	protected FilterMergeTransposeRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Filter filter = call.rel(0);
		final MergeEntities mergeEntities = call.rel(1);
		final RelBuilder relBuilder = call.builder();
		RelNode newFilterRel = filter.copy(filter.getTraitSet(), mergeEntities.getInput(), filter.getCondition()); // change input of filter with dedups
		RelNode newMergeEntitiesRel = mergeEntities.copy(mergeEntities.getTraitSet(), newFilterRel);

		call.transformTo(newMergeEntitiesRel);

	}
}
