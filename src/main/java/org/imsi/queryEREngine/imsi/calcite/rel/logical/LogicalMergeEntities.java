package org.imsi.queryEREngine.imsi.calcite.rel.logical;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCost;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptPlanner;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.imsi.calcite.rel.core.MergeEntities;

/**
 * @author bstam
 * The MergeEntities relational operator gets as input an EntityResolvedTuple 
 * and spits out just an enumerable that merges/sorts/fuses the data based on 
 * the reversed UnionFind.
 * 
 */
public class LogicalMergeEntities extends MergeEntities {

	protected LogicalMergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> projects, List<String> fieldNames) {
		super(cluster, traits, input, projects, fieldNames);
		// TODO Auto-generated constructor stub
	}

	@Override
	public LogicalMergeEntities copy(RelTraitSet traitSet, RelNode input) {
		// TODO Auto-generated method stub
		return new LogicalMergeEntities(getCluster(), traitSet, input, this.projects, this.fieldNames);
	}

	public static RelNode create(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> projects,  List<String> fieldNames) {
		// TODO Auto-generated method stub
		return new LogicalMergeEntities(cluster, traits, input, projects, fieldNames);
	}
	
	

}
