package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.core.TableScan;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.core.Deduplicate;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicate;

public class DeduplicateScanRemoveRule extends RelOptRule{
	
	public static final DeduplicateScanRemoveRule INSTANCE =
			new DeduplicateScanRemoveRule(LogicalDeduplicate.class, TableScan.class,
					RelFactories.LOGICAL_BUILDER);


	public DeduplicateScanRemoveRule(
			Class<? extends Deduplicate> deduplicateClass,
			Class<? extends TableScan> scanClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						deduplicateClass,
						operand(scanClass, any())),
				relBuilderFactory);
	}

	protected DeduplicateScanRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		
		call.transformTo(call.rel(1));

	}
}
