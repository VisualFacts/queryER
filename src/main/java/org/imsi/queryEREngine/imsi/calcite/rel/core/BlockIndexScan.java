package org.imsi.queryEREngine.imsi.calcite.rel.core;


import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCost;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptPlanner;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptTable;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.AbstractRelNode;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.apache.calcite.rel.type.RelDataType;

public class BlockIndexScan extends AbstractRelNode {
	
	protected final RelOptTable relBlockIndex;

	public BlockIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relBlockIndex) {
		super(cluster, traitSet);
		// TODO Auto-generated constructor stub
		this.relBlockIndex = relBlockIndex;
	}
	
	
	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
		return super.computeSelfCost(planner, mq);
	}



	@Override public RelDataType deriveRowType() {
		return relBlockIndex.getRowType();
	}
}
