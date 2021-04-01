package org.imsi.queryEREngine.imsi.calcite.rel.logical;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.imsi.calcite.rel.core.MergeEntities;

/**
 * @author bstam
 * The MergeEntities relational operator gets as input an EntityResolvedTuple 
 * and spits out just an enumerable that merges/sorts/fuses the data based on 
 * the reversed UnionFind.
 * 
 */
public class LogicalMergeEntities extends MergeEntities {

	protected LogicalMergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		super(cluster, traits, input);
		// TODO Auto-generated constructor stub
	}

	@Override
	public LogicalMergeEntities copy(RelTraitSet traitSet, RelNode input) {
		// TODO Auto-generated method stub
		return new LogicalMergeEntities(getCluster(), traitSet, input);
	}

	public static RelNode create(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		// TODO Auto-generated method stub
		return new LogicalMergeEntities(cluster, traits, input);
	}
	
//	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
//			RelMetadataQuery mq) {
//		// Multiply the cost by a factor that makes a scan more attractive if it
//		// has significantly fewer fields than the original scan.
//		//
//		// The "+ 2D" on top and bottom keeps the function fairly smooth.
//		//
//		// For example, if table has 3 fields, project has 1 field,
//		// then factor = (1 + 2) / (3 + 2) = 0.6
//		//System.out.println("Computing cost");
//		RelOptCost cost =  super.computeSelfCost(planner, mq)
//				.multiplyBy(0D);
//		return cost;
//	}

}
