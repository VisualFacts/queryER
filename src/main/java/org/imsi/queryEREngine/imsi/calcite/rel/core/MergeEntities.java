package org.imsi.queryEREngine.imsi.calcite.rel.core;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.SingleRel;

/**
 * 
 * @author bstam
 * This is the base Class of all MergeEntities relational operators physical or logical.
 * For calcite we need to first create a base class that extends a RelNode class and then
 * extend this class with whatever we want.
 */
public abstract class MergeEntities extends SingleRel  {

	protected MergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		super(cluster, traits, input);
		// TODO Auto-generated constructor stub
	}
	
	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, sole(inputs));
	}
	
	public abstract MergeEntities copy(RelTraitSet traitSet,  RelNode input);


}
