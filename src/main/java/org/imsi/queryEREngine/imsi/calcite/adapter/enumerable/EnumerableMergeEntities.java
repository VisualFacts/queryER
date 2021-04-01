package org.imsi.queryEREngine.imsi.calcite.adapter.enumerable;

import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableRel;
import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.PhysType;
import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptCluster;
import org.imsi.queryEREngine.apache.calcite.plan.RelTraitSet;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.metadata.RelMetadataQuery;
import org.imsi.queryEREngine.imsi.calcite.rel.core.MergeEntities;
import org.imsi.queryEREngine.imsi.calcite.util.NewBuiltInMethod;

/**
 * 
 * @author bstam
 * 
 * Physical plan implementation of MergeEntities relational operator.
 *
 */
public class EnumerableMergeEntities extends MergeEntities implements EnumerableRel {

	protected EnumerableMergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		super(cluster, traits, input);
		this.traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
	}


	@Override
	public EnumerableMergeEntities copy(RelTraitSet traitSet, RelNode input) {
		return new EnumerableMergeEntities(getCluster(), traitSet, input);
	}
	
	public static RelNode create(RelNode input) {
		final RelOptCluster cluster = input.getCluster();
		final RelMetadataQuery mq = cluster.getMetadataQuery();
		final RelTraitSet traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
		return new EnumerableMergeEntities(cluster, traitSet, input);
	}

	/**
	 * Get the entityResolvedTuple from a Deduplicate or DeduplicateJoin
	 * and merge the entities.
	 */
	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		final BlockBuilder builder = new BlockBuilder();

		final EnumerableRel child = (EnumerableRel) getInput();
		final Result result =
				implementor.visitChild(this, 0, child, pref);
		//final PhysType physType = result.physType;
		PhysType physType = PhysTypeImpl.of(
				implementor.getTypeFactory(), getRowType(), pref.prefer(result.format));

		final Expression entityResolvedTuple =
				builder.append(
						"entityResolvedTuple", result.block, false);

		builder.add(Expressions.return_(null, Expressions.call(
				NewBuiltInMethod.MERGE_ENTITIES.method,
				entityResolvedTuple
			)));

		return implementor.result(physType, builder.toBlock());
	}
}
