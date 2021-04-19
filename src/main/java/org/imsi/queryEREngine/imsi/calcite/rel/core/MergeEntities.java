package org.imsi.queryEREngine.imsi.calcite.rel.core;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCost;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptPlanner;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.SingleRel;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeFactory;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataTypeField;

public abstract class MergeEntities extends SingleRel {
	
	
	protected List<Integer> projects;
	protected List<String> fieldNames;

	protected RelDataType setType;
	protected MergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> projects, List<String> fieldNames) {
		super(cluster, traits, input);
		this.projects = projects;
		this.fieldNames = fieldNames;
		// TODO Auto-generated constructor stub
	}
	
	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, sole(inputs));
	}
	
	public abstract MergeEntities copy(RelTraitSet traitSet,  RelNode input);

	public List<Integer> getProjects() {
		return projects;
	}

	
	public List<String> getFieldNames() {
		return fieldNames;
	}
	@Override
	protected RelDataType deriveRowType() {
		if(projects==null ) return input.getRowType();
		final List<RelDataTypeField> fieldList = input.getRowType().getFieldList();
		final RelDataTypeFactory.Builder builder =
				getCluster().getTypeFactory().builder();
		for (int field : projects) {
			builder.add(fieldList.get(field));
		}
		return builder.build();
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		// Multiply the cost by a factor that makes a scan more attractive if it
		// has significantly fewer fields than the original scan.
		//
		// The "+ 2D" on top and bottom keeps the function fairly smooth.
		//
		// For example, if table has 3 fields, project has 1 field,
		// then factor = (1 + 2) / (3 + 2) = 0.6
		//System.out.println("Computing cost");
		double size = this.projects == null ? 1000000 : projects.size();
		RelOptCost cost =  super.computeSelfCost(planner, mq)
				.multiplyBy(size);
		return cost;
	}
	
}
