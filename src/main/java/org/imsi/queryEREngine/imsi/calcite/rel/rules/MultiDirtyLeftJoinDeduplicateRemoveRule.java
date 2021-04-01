package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.Join;
import org.imsi.queryEREngine.apache.calcite.rel.core.JoinRelType;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.core.Deduplicate;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicateJoin;

/**
 * 
 * @author bstam
 * An important rule that checks the join type and removes the deduplication from the corresponding
 * tablescan.
 * e.x if it is a dirtyRight join then we remove the duplication from the right
 * This rule can be used to statistically infer the best plan, deduplicate first and then join
 * or join and then deduplicate.
 */
public class MultiDirtyLeftJoinDeduplicateRemoveRule extends RelOptRule{


	public static final MultiDirtyLeftJoinDeduplicateRemoveRule INSTANCE =
			new MultiDirtyLeftJoinDeduplicateRemoveRule(LogicalDeduplicateJoin.class, 
					 Deduplicate.class, LogicalDeduplicateJoin.class,
					RelFactories.LOGICAL_BUILDER);

	/** Creates a ProjectJoinRemoveRule. */
	public MultiDirtyLeftJoinDeduplicateRemoveRule(
			Class<? extends Join> joinClass,
			Class<? extends Deduplicate> deduplicateClass,
			Class<? extends Join> joinClass2,
			RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null,
					join -> join.getJoinType() == JoinRelType.DIRTY_LEFT
							||  join.getJoinType() == JoinRelType.DIRTY
							||  join.getJoinType() == JoinRelType.CLEAN,
					operand(deduplicateClass, any()),
					operand(joinClass, any())
					),
				relBuilderFactory, null);
	}
	protected MultiDirtyLeftJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}


	/** Creates a JoinDeduplicateRemoveRule. */
	public MultiDirtyLeftJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	
	/** Creates a JoinDeduplicateRemoveRule with default factory. */
	public MultiDirtyLeftJoinDeduplicateRemoveRule(
			RelOptRuleOperand operand,
			String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Deduplicate deduplicateLeft = call.rel(1);
		
		RelNode newJoin = null;
	
		if(join.getJoinType() == JoinRelType.DIRTY_LEFT) {
			newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), join.getRight(),  join.getCondition(),
					join.getVariablesSet(), join.getJoinType(), join.getSourceLeft(), join.getSourceRight(), join.getFieldTypesLeft(), join.getFieldTypesRight(),
					join.getKeyLeft(), join.getKeyRight(), join.getTableNameLeft(), join.getTableNameRight(), join.getFieldLeft(), join.getFieldRight(), true);
		}
		else if(join.getJoinType() == JoinRelType.DIRTY) {
			newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), join.getRight(),  join.getCondition(),
					join.getVariablesSet(), JoinRelType.DIRTY_LEFT, join.getSourceLeft(), join.getSourceRight(),  join.getFieldTypesLeft(), join.getFieldTypesRight(),
					join.getKeyLeft(), join.getKeyRight(), join.getTableNameLeft(), join.getTableNameRight(), join.getFieldLeft(), join.getFieldRight(), false);
		}	
		if(newJoin != null) {
			call.transformTo(newJoin);
		}
	}

}
