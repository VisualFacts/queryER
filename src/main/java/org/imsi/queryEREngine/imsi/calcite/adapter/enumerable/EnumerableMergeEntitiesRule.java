package org.imsi.queryEREngine.imsi.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.imsi.queryEREngine.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.imsi.queryEREngine.apache.calcite.plan.Convention;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.convert.ConverterRule;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalMergeEntities;

/**
 * 
 * @author bstam
 * The rule that converts a LogicalMergeEntities to an EnumerableMergeEntitiesRule for the 
 * physical plan implementation.
 */
public class EnumerableMergeEntitiesRule extends ConverterRule {

	public EnumerableMergeEntitiesRule() {
		super(LogicalMergeEntities.class,
				(Predicate<RelNode>) r -> true,
				Convention.NONE, EnumerableConvention.INSTANCE,
				RelFactories.LOGICAL_BUILDER, "EnumerableMergeProjectRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		final LogicalMergeEntities mergeProject = (LogicalMergeEntities) rel;
		return EnumerableMergeEntities.create(
				convert(mergeProject.getInput(),
						mergeProject.getInput().getTraitSet()
						.replace(EnumerableConvention.INSTANCE)),
						mergeProject.getProjects(),
						mergeProject.getFieldNames());
	}
}
