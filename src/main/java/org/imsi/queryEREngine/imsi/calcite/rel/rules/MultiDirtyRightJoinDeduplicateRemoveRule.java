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
public class MultiDirtyRightJoinDeduplicateRemoveRule extends RelOptRule{


	public static final MultiDirtyRightJoinDeduplicateRemoveRule INSTANCE =
			new MultiDirtyRightJoinDeduplicateRemoveRule(LogicalDeduplicateJoin.class, 
					LogicalDeduplicateJoin.class, Deduplicate.class,
					RelFactories.LOGICAL_BUILDER);

	/** Creates a ProjectJoinRemoveRule. */
	public MultiDirtyRightJoinDeduplicateRemoveRule(
			Class<? extends Join> joinClass,
			Class<? extends Join> joinClass2,
			Class<? extends Deduplicate> deduplicateClass,
			RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null,
					join -> join.getJoinType() == JoinRelType.DIRTY_RIGHT
					||  join.getJoinType() == JoinRelType.DIRTY
					||  join.getJoinType() == JoinRelType.CLEAN,
					operand(joinClass, any()),
					operand(deduplicateClass, any())),
				relBuilderFactory, null);
	}
	protected MultiDirtyRightJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}


	/** Creates a JoinDeduplicateRemoveRule. */
	public MultiDirtyRightJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	
	/** Creates a JoinDeduplicateRemoveRule with default factory. */
	public MultiDirtyRightJoinDeduplicateRemoveRule(
			RelOptRuleOperand operand,
			String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Deduplicate deduplicateRight = call.rel(2);
		
		RelNode newJoin = null;
	
		if(join.getJoinType() == JoinRelType.DIRTY_RIGHT) {
			newJoin = LogicalDeduplicateJoin.create(join.getLeft(), deduplicateRight.getInput(0), join.getCondition(),
					join.getVariablesSet(), join.getJoinType(), join.getSourceLeft(), join.getSourceRight(),  join.getFieldTypesLeft(), join.getFieldTypesRight(),
					join.getKeyLeft(), join.getKeyRight(), join.getTableNameLeft(), join.getTableNameRight(), join.getFieldLeft(), join.getFieldRight(), true);
			
		}
		else if(join.getJoinType() == JoinRelType.DIRTY) {
			newJoin = LogicalDeduplicateJoin.create(join.getLeft(), deduplicateRight.getInput(0), join.getCondition(),
					join.getVariablesSet(), JoinRelType.DIRTY_RIGHT, join.getSourceLeft(), join.getSourceRight(),  join.getFieldTypesLeft(), join.getFieldTypesRight(),
					join.getKeyLeft(), join.getKeyRight(), join.getTableNameLeft(), join.getTableNameRight(), join.getFieldLeft(), join.getFieldRight(), false);
		}			
		if(newJoin != null) {
			call.transformTo(newJoin);
		}
	}

}
