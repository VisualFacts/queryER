package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.Join;
import org.imsi.queryEREngine.apache.calcite.rel.core.JoinRelType;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalJoin;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicateJoin;


public class JoinConvertToDuplicateRule extends RelOptRule{


	public static final JoinConvertToDuplicateRule INSTANCE =
			new JoinConvertToDuplicateRule(LogicalJoin.class,
					RelFactories.LOGICAL_BUILDER);

	/** Creates a ProjectJoinRemoveRule. */
	public JoinConvertToDuplicateRule(
			Class<? extends Join> joinClass,
			RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null,
					join -> join.getJoinType() == JoinRelType.CLEAN
					|| join.getJoinType() == JoinRelType.DIRTY_LEFT
					|| join.getJoinType() == JoinRelType.DIRTY
					|| join.getJoinType() == JoinRelType.DIRTY_RIGHT,
					 any()),
				relBuilderFactory, null);
	}

	/** Creates a JoinDeduplicateRemoveRule. */
	public JoinConvertToDuplicateRule(RelOptRuleOperand operand,
			String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	
	/** Creates a JoinDeduplicateRemoveRule with default factory. */
	public JoinConvertToDuplicateRule(
			RelOptRuleOperand operand,
			String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		
		RelNode newJoin = null;
		JoinRelType joinType = join.getJoinType();
		
		if(joinType != JoinRelType.CLEAN &&
				joinType != JoinRelType.DIRTY_LEFT &&
				joinType != JoinRelType.DIRTY_RIGHT &&
				joinType != JoinRelType.DIRTY) {
			return;
			
		}
		
		newJoin = LogicalDeduplicateJoin.create(join.getLeft(), join.getRight(), join.getCondition(),
				join.getVariablesSet(), join.getJoinType(), null, null, null, null,
				 null, null,  null, null, null, null, false);
		if(newJoin != null) {
			call.transformTo(newJoin);
		}
		
	}

}
