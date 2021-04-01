package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import static org.imsi.queryEREngine.apache.calcite.plan.RelOptUtil.conjunctions;

import java.util.List;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptUtil;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.Filter;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalFilter;
import org.imsi.queryEREngine.apache.calcite.rex.RexBuilder;
import org.imsi.queryEREngine.apache.calcite.rex.RexCall;
import org.imsi.queryEREngine.apache.calcite.rex.RexLiteral;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.rex.RexVisitorImpl;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.core.Deduplicate;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicate;


/**
 * 
 * @author bstam
 * Rule for transposing a filter and a deduplicate rel opt. 
 * This is to push the filter down and deduplicate a subset of the data.
 */
public class FilterDeduplicateTransposeRule extends RelOptRule {

	public static final FilterDeduplicateTransposeRule INSTANCE =
			new FilterDeduplicateTransposeRule(LogicalFilter.class, LogicalDeduplicate.class,
					RelFactories.LOGICAL_BUILDER);


	public FilterDeduplicateTransposeRule(
			Class<? extends Filter> filterClass,
			Class<? extends Deduplicate> deduplicateClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						filterClass,
						operand(deduplicateClass, any())),
				relBuilderFactory);
	}

	protected FilterDeduplicateTransposeRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	private List<RexNode> getConjunctions(Filter filter) {
		List<RexNode> conjunctions = conjunctions(filter.getCondition());
		RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
		for (int i = 0; i < conjunctions.size(); i++) {
			RexNode node = conjunctions.get(i);
			if (node instanceof RexCall) {
				conjunctions.set(i,
						RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
			}
		}
		return conjunctions;
	}
	
	private static class TokenVisitor extends RexVisitorImpl<RexLiteral> {
		private String token;

		protected TokenVisitor() {
			super(true);
		}

		public String getToken() {
			return token;
		}

		@Override public RexLiteral visitLiteral(RexLiteral literal) {
			this.token = literal.toString();
			return literal;
		}

	}
	
	public static boolean isNumeric(String strNum) {
	    if (strNum == null) {
	        return false;
	    }
	    try {
	        double d = Double.parseDouble(strNum);
	    } catch (NumberFormatException nfe) {
	        return false;
	    }
	    return true;
	}
	
	
	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Filter filter = call.rel(0);
		final Deduplicate deduplicate = call.rel(1);
		List<RexNode> conjuctions = getConjunctions(filter);
		RelNode newFilterRel = filter.copy(filter.getTraitSet(), deduplicate.getInput(0), filter.getCondition()); // change input of filter with dedups
		RelNode newDedupRel = LogicalDeduplicate.create(deduplicate.getCluster(), deduplicate.getTraitSet(),
				newFilterRel, deduplicate.getRelTable(), deduplicate.getBlockIndex(), conjuctions,
				deduplicate.getKey(), deduplicate.getSource(), deduplicate.getFieldTypes(), deduplicate.getComparisons());
		call.transformTo(newDedupRel);

	}
}
