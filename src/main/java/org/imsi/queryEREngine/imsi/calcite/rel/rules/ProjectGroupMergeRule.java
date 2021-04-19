package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import java.util.ArrayList;
import java.util.List;

import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.core.Filter;
import org.imsi.queryEREngine.apache.calcite.rel.core.Project;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.hint.RelHint;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalFilter;
import org.imsi.queryEREngine.apache.calcite.rel.logical.LogicalProject;
import org.imsi.queryEREngine.apache.calcite.rex.RexInputRef;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilder;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.apache.calcite.util.Pair;
import org.imsi.queryEREngine.imsi.calcite.adapter.enumerable.EnumerableMergeEntities;
import org.imsi.queryEREngine.imsi.calcite.rel.core.MergeEntities;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalMergeEntities;

public class ProjectGroupMergeRule extends RelOptRule {

	public static final ProjectGroupMergeRule INSTANCE =
			new ProjectGroupMergeRule(LogicalProject.class, LogicalMergeEntities.class,
					RelFactories.LOGICAL_BUILDER);


	public ProjectGroupMergeRule(
			Class<? extends Project> projectClass,
			Class<? extends MergeEntities> mergeEntitiesClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						projectClass,
						operand(mergeEntitiesClass, any())),
				relBuilderFactory);
	}

	protected ProjectGroupMergeRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Project project = call.rel(0);
		final MergeEntities mergeEntities = call.rel(1);
		List<Integer> fields = getProjectFields(project.getNamedProjects());
		List<String> fieldNames = getProjectFieldNames(project.getNamedProjects());
		LogicalMergeEntities newMergeProject = (LogicalMergeEntities) LogicalMergeEntities.create(project.getCluster(), 
				project.getTraitSet(), mergeEntities.getInput(), fields, fieldNames);
		
		call.transformTo(newMergeProject);

	}
	private List<Integer> getProjectFields(List<Pair<RexNode, String>> list) {
		final List<Integer> fields = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			final RexNode exp = list.get(i).left;
			if (exp instanceof RexInputRef) {
				fields.add(((RexInputRef) exp).getIndex());
			} else {
				return null; // not a simple projection
			}
		}

		return fields;
	}
	
	private List<String> getProjectFieldNames(List<Pair<RexNode, String>> list) {
		final List<String> fields = new ArrayList<>();
		for (int i = 0; i < list.size(); i++) {
			final String name = list.get(i).right;
			fields.add(name);
		}

		return fields;
	}
}